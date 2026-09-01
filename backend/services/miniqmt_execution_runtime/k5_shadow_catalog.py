"""K5 shadow-only view over the shared full-five catalog authority.

This module intentionally keeps its public shadow status.  Product code must
consume :mod:`full_five_catalog_authority` directly and then apply the K6 V3
materialization contract; no K5 disposition is a product execution grant.
"""

from __future__ import annotations

from dataclasses import dataclass

from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeAlgorithmBindingV2,
    VnpyFacadeConformanceAuthorityV2,
    VnpyFacadeConformanceSetV2,
    VnpyFacadeContractError,
    VnpyFacadeContractV1,
    VnpyFacadeSourceManifestV1,
)

from .full_five_catalog_authority import build_full_five_catalog_authority_v1
from .plugin_canonical import json_safe_evidence_v1
from .plugin_contracts import GatewayCapabilityCatalogV1
from .plugin_registry import PluginCatalogRuntimeV2
from backend.execution_algos.vnpy_compat.facade_characterization import VnpyFacadeCharacterizationAuthorityV2


@dataclass(frozen=True)
class K5ShadowCatalogRuntimeV1:
    """Non-product projection of the shared strict full-five authority."""

    catalog_runtime: PluginCatalogRuntimeV2
    facade_contract: VnpyFacadeContractV1
    source_manifest: VnpyFacadeSourceManifestV1
    characterization_authority: VnpyFacadeCharacterizationAuthorityV2
    k5_algorithm_bindings: tuple[VnpyFacadeAlgorithmBindingV2, ...]
    conformance_set: VnpyFacadeConformanceSetV2
    conformance_authority: VnpyFacadeConformanceAuthorityV2


def build_k5_shadow_catalog_runtime_v1(
    *,
    gateway_catalog: GatewayCapabilityCatalogV1,
) -> K5ShadowCatalogRuntimeV1:
    """Build a shadow projection without creating product-route authority."""

    authority = build_full_five_catalog_authority_v1(gateway_catalog=gateway_catalog)
    return K5ShadowCatalogRuntimeV1(
        catalog_runtime=authority.catalog_runtime,
        facade_contract=authority.facade_contract,
        source_manifest=authority.source_manifest,
        characterization_authority=authority.characterization_authority,
        k5_algorithm_bindings=authority.facade_algorithm_bindings,
        conformance_set=authority.conformance_set,
        conformance_authority=authority.conformance_authority,
    )


def readback_k5_shadow_conformance_set_v1(
    *,
    conformance_set: object,
    gateway_catalog: GatewayCapabilityCatalogV1,
) -> VnpyFacadeConformanceSetV2:
    """Strict K5 shadow readback through the one shared source authority."""

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
