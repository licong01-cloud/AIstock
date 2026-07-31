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
                "constructor_body_sha256": "f10f2658382242500244530872a5c39778d8a049f69bc9f982d988e4305c6166",
                "state_mapping_set_sha256": "da74319679cdb76af93a345fc98bfb18738544651fa05e903ba426012df7f6a1",
                "terminal_mapping_set_sha256": "337887210fbba37e4e4c4d53ec6be367bff658bb41776da0d3193a8db543cd76",
                "characterization_receipt_sha256": "d8d6560460673a0b0ef617544f594a437ffd3893e562649c30f2ad07cf854fdd",
                "adapter_contract_sha256": "869f7176dd1714ae6889d932fbb7dca525003caba4562ba2fe08b695cbeef05f",
                "source_executor_binding_sha256": "2dc3f05a23095b1e84de45275e05cedd329328c372aa53c3adb80061632bc9f8",
                "source_execution_set_sha256": "717fda4a11ed7f8612e3f0f720818d819f500a6b8a5db285beb1c5642f5d7e5e",
                "binding_sha256": "e3424b6f436697ccfb552cbba4690ccb8d9e36fe92941b51400238bf9a1e5d9d",
            }
        ),
        "STOP": freeze_json_v1(
            {
                "schema_version": "miniqmt_vnpy_facade_algorithm_binding_v2",
                "algo_code": "STOP",
                "source_identity_sha256": "bbe75b80211141ba45ab42e225c29519c895c49b7fb95bedb510f833d881d2e4",
                "class_ref": "vnpy_algotrading.algos.stop_algo:StopAlgo",
                "constructor_signature_sha256": "e0cd82b19033616a0d09d5813668064614077ab3ebc4a715e7a045b80f821393",
                "constructor_body_sha256": "85c239d8add8e9c9fa9cf7732c875bb526974069b66626f974aac22b5b1094fd",
                "state_mapping_set_sha256": "3c71151dd52c68179e35bd07cc809fc76bba434866eebab6346e957f84949924",
                "terminal_mapping_set_sha256": "35d15534a9cbff10b0e1839fcd1ee4321fe049b0505ea9ff1f19a89ff769a1fa",
                "characterization_receipt_sha256": "1ec54a4565b30bc01d3571ce1e53d7f1bcb88f2b1c71dec87b960b51c90edae2",
                "adapter_contract_sha256": "869f7176dd1714ae6889d932fbb7dca525003caba4562ba2fe08b695cbeef05f",
                "source_executor_binding_sha256": "2dc3f05a23095b1e84de45275e05cedd329328c372aa53c3adb80061632bc9f8",
                "source_execution_set_sha256": "813dbe1b9e3189b4afcf0dd4fc2989b46214bb124a289ed91335cb9c1e798031",
                "binding_sha256": "a083d2a337c21bf7f72828b61666b00e10db0018c1e66d6436ef637d6fb7be99",
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
