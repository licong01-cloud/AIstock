"""Direct K5 manifest and factory contract coverage.

These tests use the public K5 builders.  They intentionally do not import a
product composition root: K5 remains shadow-only until the later K6 cutover.
"""

from __future__ import annotations

import pytest
from types import MappingProxyType

import backend.execution_algos.vnpy_compat.k5_binding_authority as binding_module
import backend.execution_algos.vnpy_compat.k5_plugin_factories as factory_module
import backend.execution_algos.vnpy_compat.k5_plugin_manifests as manifest_module
from backend.execution_algos.vnpy_compat.facade_contracts import VnpyFacadeAlgorithmBindingV2

from backend.execution_algos.vnpy_compat.k5_binding_authority import (
    k5_facade_algorithm_bindings_v2,
)
from backend.execution_algos.vnpy_compat.k5_plugin_manifests import (
    k5_manifests_v1,
    validate_iceberg_config_v1,
    validate_stop_config_v1,
)
from backend.execution_algos.vnpy_compat.k5_plugin_factories import (
    create_iceberg_plugin_v1,
    create_stop_plugin_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import freeze_json_v1, thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    MiniQMTPluginContractError,
    MiniQMTPluginReasonCode,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    EventTypeV2,
    MarketDataCapabilityV1,
    PluginProviderV2,
)


def _manifest(algo_code: str):
    matches = [item for item in k5_manifests_v1() if item.algo_code == algo_code]
    assert len(matches) == 1
    return matches[0]


def test_k5_manifests_are_exact_iceberg_and_stop_vnpy_compat_contracts() -> None:
    manifests = k5_manifests_v1()

    assert tuple(item.algo_code for item in manifests) == ("ICEBERG", "STOP")
    assert tuple(item.plugin_id for item in manifests) == (
        "aistock.vnpy.iceberg",
        "aistock.vnpy.stop",
    )
    assert all(item.plugin_version == "1.0.0" for item in manifests)
    assert all(item.provider is PluginProviderV2.VNPY_COMPAT for item in manifests)
    assert all(item.restart_policy == "DURABLE_RESTORE" for item in manifests)

    iceberg = _manifest("ICEBERG")
    stop = _manifest("STOP")
    assert EventTypeV2.TIMER in iceberg.subscribed_event_types
    assert EventTypeV2.TICK not in iceberg.subscribed_event_types
    assert {item.capability for item in iceberg.market_data_requirements} == {
        MarketDataCapabilityV1.L1_ASK,
        MarketDataCapabilityV1.L1_BID,
    }
    assert EventTypeV2.TICK in stop.subscribed_event_types
    assert EventTypeV2.TIMER not in stop.subscribed_event_types
    assert {item.capability for item in stop.market_data_requirements} == {
        MarketDataCapabilityV1.LAST_PRICE,
        MarketDataCapabilityV1.LIMIT_UP_DOWN,
    }

    bindings = {item.algo_code: item for item in k5_facade_algorithm_bindings_v2()}
    assert iceberg.behavior_characterization_sha256 == bindings["ICEBERG"].characterization_receipt_sha256
    assert stop.behavior_characterization_sha256 == bindings["STOP"].characterization_receipt_sha256


def test_k5_config_validators_reject_aliases_nonfinite_and_wrong_carriers() -> None:
    iceberg = _manifest("ICEBERG")
    stop = _manifest("STOP")

    assert thaw_json_v1(validate_iceberg_config_v1(iceberg, {"display_volume": 100, "interval": 1}))["interval"] == 1
    assert (
        thaw_json_v1(validate_iceberg_config_v1(iceberg, {"display_volume": "100.5", "interval": 1}))["display_volume"]
        == "100.5"
    )
    assert thaw_json_v1(validate_stop_config_v1(stop, {"price_add": "-0.01"}))["price_add"] == "-0.01"

    for value in (
        {"display_volume": True, "interval": 1},
        {"display_volume": 100, "interval": False},
        {"display_volume": 100, "interval": 1, "unknown": "no"},
        {"display_volume": float("nan"), "interval": 1},
        {"display_volume": 100.5, "interval": 1},
        {"display_volume": "100", "interval": 1},
        {"display_volume": "100.50", "interval": 1},
        None,
    ):
        with pytest.raises(MiniQMTPluginContractError) as caught:
            validate_iceberg_config_v1(iceberg, value)  # type: ignore[arg-type]
        assert caught.value.reason_code is MiniQMTPluginReasonCode.CONFIG_SCHEMA_INVALID

    for validator, wrong_manifest, value in (
        (validate_iceberg_config_v1, stop, {"display_volume": 100, "interval": 1}),
        (validate_stop_config_v1, iceberg, {"price_add": "0.01"}),
    ):
        with pytest.raises(MiniQMTPluginContractError) as caught:
            validator(wrong_manifest, value)
        assert caught.value.reason_code is MiniQMTPluginReasonCode.CONFIG_SCHEMA_INVALID
    for value in (
        {"price_add": 0.01},
        {"price_add": " 0.01"},
        {"price_add": "Infinity"},
        {"price_add": "0.010"},
        {"price_add": "9" * 1000},
    ):
        with pytest.raises(MiniQMTPluginContractError) as caught:
            validate_stop_config_v1(stop, value)
        assert caught.value.reason_code is MiniQMTPluginReasonCode.CONFIG_SCHEMA_INVALID


def test_k5_factories_are_fresh_and_expose_only_immutable_conformance_binding_readback() -> None:
    iceberg_first = create_iceberg_plugin_v1({"display_volume": 100, "interval": 1})
    iceberg_second = create_iceberg_plugin_v1({"display_volume": 100, "interval": 1})
    stop_first = create_stop_plugin_v1({"price_add": "0.01"})
    stop_second = create_stop_plugin_v1({"price_add": "0.01"})

    assert iceberg_first is not iceberg_second
    assert stop_first is not stop_second
    expected = {item.algo_code: item for item in k5_facade_algorithm_bindings_v2()}
    for algo_code, adapter in (("ICEBERG", iceberg_first), ("STOP", stop_first)):
        binding, class_ref = adapter.conformance_runtime_binding_readback_v1()
        assert binding == expected[algo_code]
        assert class_ref == expected[algo_code].class_ref
    with pytest.raises(MiniQMTPluginContractError) as caught:
        create_stop_plugin_v1({"price_add": "0.010"})
    assert caught.value.reason_code is MiniQMTPluginReasonCode.CONFIG_SCHEMA_INVALID


def test_k5_binding_literal_and_factory_failures_are_typed_and_never_publish_a_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exact_payloads = binding_module._BINDING_PAYLOADS
    with pytest.raises(MiniQMTPluginContractError) as caught:
        binding_module.k5_binding_for_algo_v2("UNKNOWN")
    assert caught.value.reason_code is MiniQMTPluginReasonCode.BINDING_INVALID

    monkeypatch.setattr(
        binding_module,
        "_BINDING_PAYLOADS",
        MappingProxyType({"ICEBERG": exact_payloads["ICEBERG"]}),
    )
    with pytest.raises(MiniQMTPluginContractError) as caught:
        binding_module.k5_facade_algorithm_bindings_v2()
    assert caught.value.reason_code is MiniQMTPluginReasonCode.BINDING_INVALID

    malformed = thaw_json_v1(exact_payloads["ICEBERG"])
    malformed["binding_sha256"] = "0" * 64
    monkeypatch.setattr(
        binding_module,
        "_BINDING_PAYLOADS",
        MappingProxyType({"ICEBERG": freeze_json_v1(malformed), "STOP": exact_payloads["STOP"]}),
    )
    with pytest.raises(MiniQMTPluginContractError) as caught:
        binding_module.k5_facade_algorithm_bindings_v2()
    assert caught.value.reason_code is MiniQMTPluginReasonCode.BINDING_INVALID

    iceberg = VnpyFacadeAlgorithmBindingV2.model_validate(thaw_json_v1(exact_payloads["ICEBERG"]), strict=True)
    wrong_identity = VnpyFacadeAlgorithmBindingV2.create(
        **{
            **iceberg.canonical_payload_v1(exclude={"binding_sha256"}),
            "algo_code": "STOP",
        }
    )
    monkeypatch.setattr(
        binding_module,
        "_BINDING_PAYLOADS",
        MappingProxyType(
            {
                "ICEBERG": freeze_json_v1(wrong_identity.canonical_payload_v1()),
                "STOP": exact_payloads["STOP"],
            }
        ),
    )
    with pytest.raises(MiniQMTPluginContractError) as caught:
        binding_module.k5_facade_algorithm_bindings_v2()
    assert caught.value.reason_code is MiniQMTPluginReasonCode.BINDING_INVALID

    monkeypatch.setattr(binding_module, "_BINDING_PAYLOADS", exact_payloads)
    monkeypatch.setattr(manifest_module, "k5_manifests_v1", lambda: ())
    with pytest.raises(MiniQMTPluginContractError) as caught:
        manifest_module.validate_stop_config_v1(_manifest("STOP"), {"price_add": "0.01"})
    assert caught.value.reason_code is MiniQMTPluginReasonCode.MANIFEST_SCHEMA_INVALID

    monkeypatch.undo()
    monkeypatch.setattr(factory_module, "k5_manifests_v1", lambda: ())
    with pytest.raises(MiniQMTPluginContractError) as caught:
        factory_module.create_stop_plugin_v1({"price_add": "0.01"})
    assert caught.value.reason_code is MiniQMTPluginReasonCode.BINDING_INVALID

    monkeypatch.undo()
    monkeypatch.setattr(factory_module, "load_pinned_vnpy_algorithm_classes_v1", lambda: {})
    with pytest.raises(MiniQMTPluginContractError) as caught:
        factory_module.create_stop_plugin_v1({"price_add": "0.01"})
    assert caught.value.reason_code is MiniQMTPluginReasonCode.BINDING_INVALID
