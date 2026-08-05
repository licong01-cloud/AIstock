"""K5 code-owned V2 binding literals for the pinned Iceberg and Stop algorithms.

This module is deliberately pure: literals are strict-read into the existing
K4 carrier, never treated as a characterization success result, and are
compared with a freshly executed K4 authority by the K5 composition root.
"""

from __future__ import annotations

from types import MappingProxyType

from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    freeze_json_v1,
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    MiniQMTPluginContractError,
    MiniQMTPluginReasonCode,
)

from .facade_contracts import VnpyFacadeAlgorithmBindingV2


_EXPECTED_ALGO_CODES = ("ICEBERG", "STOP")

# These are code-owned copies of the exact V2 bindings regenerated from the
# K4 fresh five-algorithm characterization authority.  They do not contain a
# PASSED flag and cannot replace that authority during K5 catalog publication.
_BINDING_PAYLOADS = MappingProxyType(
    {
        "ICEBERG": freeze_json_v1(
            {
                "schema_version": "miniqmt_vnpy_facade_algorithm_binding_v2",
                "algo_code": "ICEBERG",
                "source_identity_sha256": "32b001ce7d122e7187aa6fc40bce4f12decc2c7e2dc4330006e67ac20be5a11c",
                "class_ref": "vnpy_algotrading.algos.iceberg_algo:IcebergAlgo",
                "constructor_signature_sha256": "e0cd82b19033616a0d09d5813668064614077ab3ebc4a715e7a045b80f821393",
                "constructor_body_sha256": "5438bb6d1ca6e6e3e130ef4e9e492e6372487e4e28fa81891e305e0aba21f442",
                "state_mapping_set_sha256": "da74319679cdb76af93a345fc98bfb18738544651fa05e903ba426012df7f6a1",
                "terminal_mapping_set_sha256": "337887210fbba37e4e4c4d53ec6be367bff658bb41776da0d3193a8db543cd76",
                "characterization_receipt_sha256": "58e435b8050f912c8163411f45b44819a3adabb56b0f2316ed667c87a70cb5a8",
                "adapter_contract_sha256": "0158a2808d4673166edd721bf321d0457e4ee7ef2e9b4537187b5c7809914cad",
                "source_executor_binding_sha256": "f92c35a7a9cabede07c8e77da09f6779e6f6393a5cc36b00de1daefc743d8281",
                "source_execution_set_sha256": "15c193afbc9f8774604e7999646e0e01c517b82244a4b457a3ac2fbc11a031ea",
                "binding_sha256": "99e6382468139063844813bb89f910182cc99ca64e48399ea8575ea21b3cd499",
            }
        ),
        "STOP": freeze_json_v1(
            {
                "schema_version": "miniqmt_vnpy_facade_algorithm_binding_v2",
                "algo_code": "STOP",
                "source_identity_sha256": "bbe75b80211141ba45ab42e225c29519c895c49b7fb95bedb510f833d881d2e4",
                "class_ref": "vnpy_algotrading.algos.stop_algo:StopAlgo",
                "constructor_signature_sha256": "e0cd82b19033616a0d09d5813668064614077ab3ebc4a715e7a045b80f821393",
                "constructor_body_sha256": "fde0565d377dae82c85f103fe27292ee85430f86ed0d74b99c9235091e964844",
                "state_mapping_set_sha256": "3c71151dd52c68179e35bd07cc809fc76bba434866eebab6346e957f84949924",
                "terminal_mapping_set_sha256": "35d15534a9cbff10b0e1839fcd1ee4321fe049b0505ea9ff1f19a89ff769a1fa",
                "characterization_receipt_sha256": "8b5113e863a2fe270bd0a798562bcfa52c802ead8fff5b097a2db8b7da24586a",
                "adapter_contract_sha256": "0158a2808d4673166edd721bf321d0457e4ee7ef2e9b4537187b5c7809914cad",
                "source_executor_binding_sha256": "f92c35a7a9cabede07c8e77da09f6779e6f6393a5cc36b00de1daefc743d8281",
                "source_execution_set_sha256": "5465e4f0647aa5db268938cf7a18275a3557e0e30381d40df98c738c80ae7d55",
                "binding_sha256": "463b9e67d3a11ba109840b7572fd4e145b0fdf620579cf5cf01acd50214516e4",
            }
        ),
    }
)


def k5_facade_algorithm_bindings_v2() -> tuple[VnpyFacadeAlgorithmBindingV2, ...]:
    """Strictly reconstruct the exact immutable K5 binding pair."""

    if tuple(sorted(_BINDING_PAYLOADS)) != _EXPECTED_ALGO_CODES:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.BINDING_INVALID,
            "K5 binding literal catalog must contain exactly ICEBERG and STOP",
            context={
                "expected_algo_codes": _EXPECTED_ALGO_CODES,
                "actual_algo_codes": tuple(sorted(_BINDING_PAYLOADS)),
            },
        )
    try:
        bindings = tuple(
            VnpyFacadeAlgorithmBindingV2.model_validate(thaw_json_v1(_BINDING_PAYLOADS[algo_code]), strict=True)
            for algo_code in _EXPECTED_ALGO_CODES
        )
    except (TypeError, ValueError) as exc:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.BINDING_INVALID,
            "K5 binding literal strict readback failed",
            context={"expected_algo_codes": _EXPECTED_ALGO_CODES, "error_type": type(exc).__name__, "error": str(exc)},
        ) from exc
    if tuple(item.algo_code for item in bindings) != _EXPECTED_ALGO_CODES:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.BINDING_INVALID,
            "K5 binding literal algorithm identities are not canonical",
            context={
                "expected_algo_codes": _EXPECTED_ALGO_CODES,
                "actual_algo_codes": tuple(item.algo_code for item in bindings),
            },
        )
    return bindings


def k5_binding_for_algo_v2(algo_code: str) -> VnpyFacadeAlgorithmBindingV2:
    """Return one exact binding; unsupported identities fail loudly."""

    if type(algo_code) is not str or algo_code not in _EXPECTED_ALGO_CODES:
        raise MiniQMTPluginContractError(
            MiniQMTPluginReasonCode.BINDING_INVALID,
            "K5 facade binding requires exact ICEBERG or STOP algo_code",
            context={"expected_algo_codes": _EXPECTED_ALGO_CODES, "actual_algo_code": algo_code},
        )
    return next(item for item in k5_facade_algorithm_bindings_v2() if item.algo_code == algo_code)


__all__ = ["k5_binding_for_algo_v2", "k5_facade_algorithm_bindings_v2"]
