from __future__ import annotations

import subprocess
import sys
from itertools import permutations
from pathlib import Path

import pytest

from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_creation_bindings_v1,
    current_three_descriptors_v2,
    current_three_manifests_v2,
    current_three_process_bindings_v2,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import canonical_json_bytes_v1, hash_hex_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    GatewayCapabilityCatalogV1,
    MarketDataCapabilityV1,
    MiniQMTPluginReasonCode,
    OrderTypeV1,
    SessionPhaseV1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import (
    CatalogBuildStageV1,
    CompatibilityStatusV1,
    PluginCatalogBuildError,
    PluginCatalogBuildFailureReceiptV1,
    PluginCatalogBuildFailureV1,
    PluginCreationBindingV1,
    PluginKeyV1,
    VnpyCompatibilityFailureV1,
    VnpyCompatibilityReceiptV1,
    build_plugin_catalog_v2,
    compatibility_component_hashes_v1,
    evaluate_plugin_route_compatibility_v1,
)


def _receipts(failed_plugin_id: str | None = None) -> tuple[VnpyCompatibilityReceiptV1, ...]:
    result = []
    for manifest in current_three_manifests_v2():
        components = compatibility_component_hashes_v1(manifest.compatibility_requirement)
        failed = manifest.plugin_id == failed_plugin_id
        failures = (
            VnpyCompatibilityFailureV1.create(
                field_path="required_method_signatures",
                reason_code="TEST_PINNED_SURFACE_MISMATCH",
                context={"plugin_id": manifest.plugin_id},
            ),
        ) if failed else ()
        result.append(
            VnpyCompatibilityReceiptV1.create(
                plugin_id=manifest.plugin_id,
                plugin_version=manifest.plugin_version,
                manifest_sha256=manifest.manifest_sha256,
                requirement_sha256=manifest.compatibility_requirement.requirement_sha256,
                surface_sha256=components["surface_sha256"],
                source_lock_sha256=components["source_lock_sha256"],
                method_signature_sha256=components["method_signature_sha256"],
                object_field_sha256=components["object_field_sha256"],
                characterization_sha256=manifest.compatibility_requirement.characterization_sha256,
                status=CompatibilityStatusV1.FAILED if failed else CompatibilityStatusV1.PASSED,
                ordered_failures=failures,
            )
        )
    return tuple(result)


def _build(receipts: tuple[VnpyCompatibilityReceiptV1, ...] | None = None):
    return build_plugin_catalog_v2(
        descriptors=current_three_descriptors_v2(),
        creation_bindings=current_three_creation_bindings_v1(),
        process_bindings=current_three_process_bindings_v2(),
        pinned_compatibility_receipts=_receipts() if receipts is None else receipts,
    )


def _gateway(capabilities: tuple[MarketDataCapabilityV1, ...]) -> GatewayCapabilityCatalogV1:
    payload = {
        "schema_version": "miniqmt_gateway_capability_catalog_v1",
        "route_id": "route.sim.primary",
        "quote_source": "B0_QUOTE_V2",
        "gateway_backend": "minqmt_sim",
        "order_types": (OrderTypeV1.LIMIT,),
        "market_data_capabilities": capabilities,
        "session_phases": (SessionPhaseV1.CONTINUOUS_AM, SessionPhaseV1.CONTINUOUS_PM),
        "idempotent_submit_by_client_ref": False,
        "exact_order_id_cancel": True,
    }
    payload["catalog_sha256"] = hash_hex_v1(
        "miniqmt_gateway_capability_catalog_v1",
        {
            key: sorted(item.value for item in value) if isinstance(value, tuple) else value
            for key, value in payload.items()
        },
    )
    return GatewayCapabilityCatalogV1(**payload)


def test_catalog_snapshot_is_byte_identical_for_all_input_permutations() -> None:
    descriptors = current_three_descriptors_v2()
    expected = _build().snapshot
    expected_bytes = canonical_json_bytes_v1(expected.canonical_payload_v1())
    for descriptor_order in permutations(descriptors):
        runtime = build_plugin_catalog_v2(
            descriptors=descriptor_order,
            creation_bindings=tuple(reversed(current_three_creation_bindings_v1())),
            process_bindings=current_three_process_bindings_v2(),
            pinned_compatibility_receipts=tuple(reversed(_receipts())),
        )
        assert runtime.snapshot.catalog_sha256 == expected.catalog_sha256
        assert canonical_json_bytes_v1(runtime.snapshot.canonical_payload_v1()) == expected_bytes


def test_snapshot_has_no_callable_and_process_mapping_is_sealed() -> None:
    bindings = current_three_process_bindings_v2()
    original = bindings.resolve("aistock.vnpy.sniper.factory")
    caller_copy = bindings.copy_bindings_v1()
    caller_copy["aistock.vnpy.sniper.factory"] = lambda: None
    snapshot_bytes = canonical_json_bytes_v1(_build().snapshot.canonical_payload_v1())
    assert b"<function" not in snapshot_bytes
    assert b"0x" not in snapshot_bytes
    assert b"pid" not in snapshot_bytes.lower()
    assert bindings.resolve("aistock.vnpy.sniper.factory") is original


def test_catalog_snapshot_is_byte_identical_in_a_fresh_process() -> None:
    expected = canonical_json_bytes_v1(_build().snapshot.canonical_payload_v1()).hex()
    code = """
from backend.execution_algos.vnpy_style.plugin_manifests import current_three_creation_bindings_v1, current_three_descriptors_v2, current_three_manifests_v2, current_three_process_bindings_v2
from backend.services.miniqmt_execution_runtime.plugin_canonical import canonical_json_bytes_v1
from backend.services.miniqmt_execution_runtime.plugin_registry import CompatibilityStatusV1, VnpyCompatibilityReceiptV1, build_plugin_catalog_v2, compatibility_component_hashes_v1
receipts = []
for manifest in current_three_manifests_v2():
    c = compatibility_component_hashes_v1(manifest.compatibility_requirement)
    receipts.append(VnpyCompatibilityReceiptV1.create(plugin_id=manifest.plugin_id, plugin_version=manifest.plugin_version, manifest_sha256=manifest.manifest_sha256, requirement_sha256=manifest.compatibility_requirement.requirement_sha256, surface_sha256=c['surface_sha256'], source_lock_sha256=c['source_lock_sha256'], method_signature_sha256=c['method_signature_sha256'], object_field_sha256=c['object_field_sha256'], characterization_sha256=manifest.compatibility_requirement.characterization_sha256, status=CompatibilityStatusV1.PASSED, ordered_failures=()))
runtime = build_plugin_catalog_v2(descriptors=current_three_descriptors_v2(), creation_bindings=current_three_creation_bindings_v1(), process_bindings=current_three_process_bindings_v2(), pinned_compatibility_receipts=tuple(receipts))
print(canonical_json_bytes_v1(runtime.snapshot.canonical_payload_v1()).hex())
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path(__file__).resolve().parents[3],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == expected


def test_creation_binding_and_restore_use_exact_plugin_keys() -> None:
    runtime = _build()
    key = runtime.plugin_key_for_new_instance("SNIPER_MINIQMT")
    expected = next(item for item in current_three_creation_bindings_v1() if item.algo_code == "SNIPER_MINIQMT")
    assert key == expected.plugin_key
    assert runtime.descriptor_for_restore(key).manifest.plugin_id == "aistock.vnpy.sniper"
    with pytest.raises(KeyError):
        runtime.descriptor_for_restore(
            PluginKeyV1(plugin_id=key.plugin_id, plugin_version=key.plugin_version, manifest_sha256="f" * 64)
        )


def test_catalog_aggregates_failures_and_never_publishes_partial_state() -> None:
    creation = current_three_creation_bindings_v1()
    duplicate = PluginCreationBindingV1(algo_code=creation[0].algo_code, plugin_key=creation[0].plugin_key)
    process = current_three_process_bindings_v2().without("aistock.vnpy.best_limit.state_codec")
    with pytest.raises(PluginCatalogBuildError) as error:
        build_plugin_catalog_v2(
            descriptors=current_three_descriptors_v2(),
            creation_bindings=creation + (duplicate,),
            process_bindings=process,
            pinned_compatibility_receipts=_receipts("aistock.vnpy.twap_lite"),
        )
    receipt = error.value.receipt
    assert receipt.total_failure_count >= 3
    assert receipt.ordered_failures == tuple(sorted(receipt.ordered_failures, key=lambda item: item.sort_key_v1()))
    assert {item.stage for item in receipt.ordered_failures}.issuperset(
        {
            CatalogBuildStageV1.PROCESS_BINDING,
            CatalogBuildStageV1.PINNED_COMPATIBILITY,
            CatalogBuildStageV1.REGISTRATION_CREATION,
        }
    )
    assert receipt.failure_set_sha256 != receipt.receipt_sha256
    assert error.value.partial_catalog is None


def test_empty_catalog_and_orphan_receipt_fail_without_partial_publication() -> None:
    with pytest.raises(PluginCatalogBuildError) as empty_error:
        build_plugin_catalog_v2(
            descriptors=(),
            creation_bindings=(),
            process_bindings=current_three_process_bindings_v2(),
            pinned_compatibility_receipts=(),
        )
    assert empty_error.value.partial_catalog is None
    assert any(item.field_path == "descriptors" for item in empty_error.value.receipt.ordered_failures)

    manifest = current_three_manifests_v2()[0]
    components = compatibility_component_hashes_v1(manifest.compatibility_requirement)
    orphan = VnpyCompatibilityReceiptV1.create(
        plugin_id="aistock.vnpy.orphan",
        plugin_version=manifest.plugin_version,
        manifest_sha256=manifest.manifest_sha256,
        requirement_sha256=manifest.compatibility_requirement.requirement_sha256,
        surface_sha256=components["surface_sha256"],
        source_lock_sha256=components["source_lock_sha256"],
        method_signature_sha256=components["method_signature_sha256"],
        object_field_sha256=components["object_field_sha256"],
        characterization_sha256=manifest.compatibility_requirement.characterization_sha256,
        status=CompatibilityStatusV1.PASSED,
        ordered_failures=(),
    )
    with pytest.raises(PluginCatalogBuildError) as orphan_error:
        _build((*_receipts(), orphan))
    assert any("orphan_receipt" in str(item.canonical_payload_v1()) for item in orphan_error.value.receipt.ordered_failures)


def test_missing_or_failed_pinned_receipt_cannot_be_defaulted_to_passed() -> None:
    for receipts in (_receipts()[:-1], _receipts("aistock.vnpy.sniper")):
        with pytest.raises(PluginCatalogBuildError) as error:
            _build(receipts)
        assert any(item.stage is CatalogBuildStageV1.PINNED_COMPATIBILITY for item in error.value.receipt.ordered_failures)


def test_pinned_receipt_component_hash_mismatch_fails_loud() -> None:
    receipts = list(_receipts())
    manifest = current_three_manifests_v2()[0]
    components = compatibility_component_hashes_v1(manifest.compatibility_requirement)
    receipts[0] = VnpyCompatibilityReceiptV1.create(
        plugin_id=manifest.plugin_id,
        plugin_version=manifest.plugin_version,
        manifest_sha256=manifest.manifest_sha256,
        requirement_sha256=manifest.compatibility_requirement.requirement_sha256,
        surface_sha256="f" * 64,
        source_lock_sha256=components["source_lock_sha256"],
        method_signature_sha256=components["method_signature_sha256"],
        object_field_sha256=components["object_field_sha256"],
        characterization_sha256=manifest.compatibility_requirement.characterization_sha256,
        status=CompatibilityStatusV1.PASSED,
        ordered_failures=(),
    )
    with pytest.raises(PluginCatalogBuildError) as error:
        _build(tuple(receipts))
    assert any(item.stage is CatalogBuildStageV1.PINNED_COMPATIBILITY for item in error.value.receipt.ordered_failures)


def test_aggregate_failure_receipt_is_bounded_and_hashes_omitted_set() -> None:
    failures = [
        PluginCatalogBuildFailureV1.create(
            stage=CatalogBuildStageV1.SNAPSHOT_FREEZE,
            descriptor=None,
            field_path=f"failure_{index:03d}",
            reason_code=MiniQMTPluginReasonCode.REGISTRATION_CONFLICT,
            context={"index": index},
        )
        for index in range(300)
    ]
    receipt = PluginCatalogBuildFailureReceiptV1.create(
        build_input_sha256="a" * 64,
        descriptor_keys=(),
        failures=failures,
    )
    assert receipt.total_failure_count == 300
    assert receipt.failures_truncated is True
    assert len(receipt.ordered_failures) == 256
    assert receipt.omitted_failure_set_sha256 is not None
    assert any(item.field_path == "__failure_set__" for item in receipt.ordered_failures)


def test_route_failure_isolated_from_catalog_and_other_plugins() -> None:
    runtime = _build()
    sniper = evaluate_plugin_route_compatibility_v1(
        catalog_snapshot=runtime.snapshot,
        plugin_key=runtime.plugin_key_for_new_instance("SNIPER_MINIQMT"),
        gateway_catalog=_gateway((MarketDataCapabilityV1.L1_BID,)),
    )
    best = evaluate_plugin_route_compatibility_v1(
        catalog_snapshot=runtime.snapshot,
        plugin_key=runtime.plugin_key_for_new_instance("BEST_LIMIT_MINIQMT"),
        gateway_catalog=_gateway((MarketDataCapabilityV1.L1_BID, MarketDataCapabilityV1.L1_ASK)),
    )
    assert sniper.status is CompatibilityStatusV1.FAILED
    assert sniper.broker_called is False
    assert all(item.kind == "STATIC_UNSUPPORTED" for item in sniper.ordered_failures)
    assert best.status is CompatibilityStatusV1.PASSED
    assert runtime.snapshot.catalog_sha256 == _build().snapshot.catalog_sha256
    assert type(sniper).model_validate_json(sniper.model_dump_json()) == sniper
