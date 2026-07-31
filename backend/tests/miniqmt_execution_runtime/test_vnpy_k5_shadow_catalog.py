"""K5 full-five shadow composition tests."""

from __future__ import annotations

import pytest

import backend.services.miniqmt_execution_runtime.k5_shadow_catalog as shadow_module
from backend.execution_algos.vnpy_compat.facade_contracts import VnpyFacadeContractError
from backend.services.miniqmt_execution_runtime.k5_shadow_catalog import (
    build_k5_shadow_catalog_runtime_v1,
    readback_k5_shadow_conformance_set_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    GatewayCapabilityCatalogV1,
    MarketDataCapabilityV1,
    OrderTypeV1,
    SessionPhaseV1,
)


def _gateway(route_id: str = "route.k5.shadow.test") -> GatewayCapabilityCatalogV1:
    values = {
        "schema_version": "miniqmt_gateway_capability_catalog_v1",
        "route_id": route_id,
        "quote_source": "B0_QUOTE_V2",
        "gateway_backend": "minqmt_sim",
        "order_types": tuple(sorted(OrderTypeV1, key=lambda item: item.value)),
        "market_data_capabilities": tuple(sorted(MarketDataCapabilityV1, key=lambda item: item.value)),
        "session_phases": tuple(sorted(SessionPhaseV1, key=lambda item: item.value)),
        "idempotent_submit_by_client_ref": False,
        "exact_order_id_cancel": True,
    }
    payload = {
        **values,
        "order_types": [item.value for item in values["order_types"]],
        "market_data_capabilities": [item.value for item in values["market_data_capabilities"]],
        "session_phases": [item.value for item in values["session_phases"]],
    }
    return GatewayCapabilityCatalogV1(
        **values,
        catalog_sha256=hash_hex_v1("miniqmt_gateway_capability_catalog_v1", payload),
    )


def test_k5_shadow_catalog_rebuilds_exact_full_five_after_fresh_k4_authority() -> None:
    candidate = build_k5_shadow_catalog_runtime_v1(gateway_catalog=_gateway())

    assert tuple(item.manifest.algo_code for item in candidate.catalog_runtime.snapshot.registration_descriptors) == (
        "BEST_LIMIT_MINIQMT",
        "ICEBERG",
        "SNIPER_MINIQMT",
        "STOP",
        "TWAP_LITE_MINIQMT",
    )
    assert tuple(item.algo_code for item in candidate.k5_algorithm_bindings) == ("ICEBERG", "STOP")
    assert tuple(item.algo_code for item in candidate.characterization_authority.receipts) == (
        "BEST_LIMIT_MINIQMT",
        "ICEBERG",
        "SNIPER_MINIQMT",
        "STOP",
        "TWAP_LITE_MINIQMT",
    )
    assert tuple(item.runtime_binding_disposition.value for item in candidate.conformance_set.ordered_receipts) == (
        "PURE_PLUGIN_SHADOW_CONFORMANCE",
        "FACADE_BACKED_ADAPTER",
        "PURE_PLUGIN_SHADOW_CONFORMANCE",
        "FACADE_BACKED_ADAPTER",
        "PURE_PLUGIN_SHADOW_CONFORMANCE",
    )
    assert (
        readback_k5_shadow_conformance_set_v1(
            conformance_set=candidate.conformance_set,
            gateway_catalog=_gateway(),
        )
        == candidate.conformance_set
    )


def test_k5_shadow_catalog_rejects_literal_drift_and_authority_readback_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = build_k5_shadow_catalog_runtime_v1(gateway_catalog=_gateway())
    monkeypatch.setattr(
        shadow_module,
        "k5_facade_algorithm_bindings_v2",
        lambda: candidate.k5_algorithm_bindings[:1],
    )
    with pytest.raises(VnpyFacadeContractError) as caught:
        build_k5_shadow_catalog_runtime_v1(gateway_catalog=_gateway())
    assert caught.value.reason_code == "MINIQMT_VNPY_FACADE_BINDING_INVALID"

    monkeypatch.undo()
    drifted_payload = candidate.conformance_set.model_dump(mode="python")
    drifted_payload["plugin_catalog_sha256"] = "f" * 64
    hash_payload = candidate.conformance_set.canonical_payload_v1(exclude={"receipt_set_sha256"})
    hash_payload["plugin_catalog_sha256"] = "f" * 64
    drifted_payload["receipt_set_sha256"] = hash_hex_v1(
        "miniqmt_vnpy_facade_conformance_set_v2",
        hash_payload,
    )
    drifted = type(candidate.conformance_set).model_validate(drifted_payload, strict=True)
    with pytest.raises(VnpyFacadeContractError) as caught:
        readback_k5_shadow_conformance_set_v1(
            conformance_set=drifted,
            gateway_catalog=_gateway(),
        )
    assert caught.value.reason_code == "MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID"
