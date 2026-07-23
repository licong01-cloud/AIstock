from __future__ import annotations

import json
import subprocess
import sys
from itertools import permutations
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_creation_bindings_v1,
    current_three_descriptors_v2,
    current_three_manifests_v2,
    current_three_process_bindings_v2,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    canonical_json_bytes_v1,
    hash_hex_v1,
    thaw_json_v1,
)
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
    PluginCatalogSnapshotV1,
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
            (
                VnpyCompatibilityFailureV1.create(
                    field_path="required_method_signatures",
                    reason_code="TEST_PINNED_SURFACE_MISMATCH",
                    context={"plugin_id": manifest.plugin_id},
                ),
            )
            if failed
            else ()
        )
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


def _gateway(
    capabilities: tuple[MarketDataCapabilityV1, ...],
    *,
    quote_source: str = "B0_QUOTE_V2",
    exact_order_id_cancel: bool = True,
) -> GatewayCapabilityCatalogV1:
    payload = {
        "schema_version": "miniqmt_gateway_capability_catalog_v1",
        "route_id": "route.sim.primary",
        "quote_source": quote_source,
        "gateway_backend": "minqmt_sim",
        "order_types": (OrderTypeV1.LIMIT,),
        "market_data_capabilities": capabilities,
        "session_phases": (SessionPhaseV1.CONTINUOUS_AM, SessionPhaseV1.CONTINUOUS_PM),
        "idempotent_submit_by_client_ref": False,
        "exact_order_id_cancel": exact_order_id_cancel,
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


def test_catalog_snapshot_readback_rejects_noncanonical_registration_order() -> None:
    snapshot = _build().snapshot
    descriptors = tuple(reversed(snapshot.registration_descriptors))
    hash_payload = {
        "schema_version": "plugin_catalog_snapshot_v1",
        "registration_descriptors": [item.canonical_payload_v1() for item in descriptors],
        "pinned_compatibility_receipts": [
            item.canonical_payload_v1() for item in snapshot.pinned_compatibility_receipts
        ],
        "creation_bindings": [item.canonical_payload_v1() for item in snapshot.creation_bindings],
    }
    payload = {
        "schema_version": "plugin_catalog_snapshot_v1",
        "registration_descriptors": descriptors,
        "pinned_compatibility_receipts": snapshot.pinned_compatibility_receipts,
        "creation_bindings": snapshot.creation_bindings,
    }
    payload["catalog_sha256"] = hash_hex_v1(
        "miniqmt_plugin_catalog_snapshot_v1",
        hash_payload,
    )

    with pytest.raises(ValueError, match="canonical|sorted"):
        PluginCatalogSnapshotV1.model_validate(payload, strict=True)


def test_catalog_snapshot_readback_rejects_hash_correct_duplicate_registration() -> None:
    snapshot = _build().snapshot
    descriptors = (snapshot.registration_descriptors[0], *snapshot.registration_descriptors)
    hash_payload = {
        "schema_version": "plugin_catalog_snapshot_v1",
        "registration_descriptors": [item.canonical_payload_v1() for item in descriptors],
        "pinned_compatibility_receipts": [
            item.canonical_payload_v1() for item in snapshot.pinned_compatibility_receipts
        ],
        "creation_bindings": [item.canonical_payload_v1() for item in snapshot.creation_bindings],
    }
    payload = {
        "schema_version": "plugin_catalog_snapshot_v1",
        "registration_descriptors": descriptors,
        "pinned_compatibility_receipts": snapshot.pinned_compatibility_receipts,
        "creation_bindings": snapshot.creation_bindings,
    }
    payload["catalog_sha256"] = hash_hex_v1(
        "miniqmt_plugin_catalog_snapshot_v1",
        hash_payload,
    )

    with pytest.raises(ValueError, match="duplicate|unique"):
        PluginCatalogSnapshotV1.model_validate(payload, strict=True)


def test_catalog_snapshot_readback_rejects_incomplete_compatibility_component_closure() -> None:
    snapshot = _build().snapshot
    receipt = snapshot.pinned_compatibility_receipts[0]
    receipt_fields = {
        "requirement_sha256": receipt.requirement_sha256,
        "surface_sha256": receipt.surface_sha256,
        "source_lock_sha256": receipt.source_lock_sha256,
        "method_signature_sha256": receipt.method_signature_sha256,
        "object_field_sha256": receipt.object_field_sha256,
        "characterization_sha256": receipt.characterization_sha256,
    }
    for corrupted_field in receipt_fields:
        corrupted_fields = {**receipt_fields, corrupted_field: "f" * 64}
        corrupted = VnpyCompatibilityReceiptV1.create(
            plugin_id=receipt.plugin_id,
            plugin_version=receipt.plugin_version,
            manifest_sha256=receipt.manifest_sha256,
            **corrupted_fields,
            status=CompatibilityStatusV1.PASSED,
            ordered_failures=(),
        )
        receipts = (corrupted, *snapshot.pinned_compatibility_receipts[1:])
        payload = {
            "schema_version": "plugin_catalog_snapshot_v1",
            "registration_descriptors": snapshot.registration_descriptors,
            "pinned_compatibility_receipts": receipts,
            "creation_bindings": snapshot.creation_bindings,
        }
        payload["catalog_sha256"] = hash_hex_v1(
            "miniqmt_plugin_catalog_snapshot_v1",
            {
                "schema_version": payload["schema_version"],
                "registration_descriptors": [item.canonical_payload_v1() for item in snapshot.registration_descriptors],
                "pinned_compatibility_receipts": [item.canonical_payload_v1() for item in receipts],
                "creation_bindings": [item.canonical_payload_v1() for item in snapshot.creation_bindings],
            },
        )

        with pytest.raises(ValueError, match="compatibility.*closure|component"):
            PluginCatalogSnapshotV1.model_validate(payload, strict=True)


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


def test_malformed_descriptor_is_aggregated_without_hiding_later_failures() -> None:
    descriptors = current_three_descriptors_v2()
    malformed = descriptors[0].model_copy(update={"manifest": "malformed"})
    process = current_three_process_bindings_v2().without("aistock.vnpy.best_limit.state_codec")

    with pytest.raises(PluginCatalogBuildError) as error:
        build_plugin_catalog_v2(
            descriptors=(malformed, *descriptors[1:]),
            creation_bindings=current_three_creation_bindings_v1(),
            process_bindings=process,
            pinned_compatibility_receipts=_receipts(),
        )

    stages = {item.stage for item in error.value.receipt.ordered_failures}
    assert CatalogBuildStageV1.STRICT_PARSE in stages
    assert CatalogBuildStageV1.PROCESS_BINDING in stages
    assert error.value.partial_catalog is None


def test_failure_receipt_is_input_order_independent_for_semantic_conflicts() -> None:
    creation = current_three_creation_bindings_v1()
    duplicate = PluginCreationBindingV1(algo_code=creation[0].algo_code, plugin_key=creation[0].plugin_key)
    receipts = []
    for ordered_creation in (creation + (duplicate,), (duplicate,) + creation):
        with pytest.raises(PluginCatalogBuildError) as error:
            build_plugin_catalog_v2(
                descriptors=current_three_descriptors_v2(),
                creation_bindings=ordered_creation,
                process_bindings=current_three_process_bindings_v2(),
                pinned_compatibility_receipts=_receipts(),
            )
        receipts.append(error.value.receipt)

    assert receipts[0].build_input_sha256 == receipts[1].build_input_sha256
    assert receipts[0].failure_set_sha256 == receipts[1].failure_set_sha256
    assert receipts[0].receipt_sha256 == receipts[1].receipt_sha256


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
    assert any(
        "orphan_receipt" in str(item.canonical_payload_v1()) for item in orphan_error.value.receipt.ordered_failures
    )


def test_missing_or_failed_pinned_receipt_cannot_be_defaulted_to_passed() -> None:
    for receipts in (_receipts()[:-1], _receipts("aistock.vnpy.sniper")):
        with pytest.raises(PluginCatalogBuildError) as error:
            _build(receipts)
        assert any(
            item.stage is CatalogBuildStageV1.PINNED_COMPATIBILITY for item in error.value.receipt.ordered_failures
        )


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
    assert receipt.ordered_failures[-1].field_path == "__failure_set__"
    assert PluginCatalogBuildFailureReceiptV1.model_validate_json(receipt.model_dump_json()) == receipt


def test_failure_receipt_rejects_unreported_nontruncated_failures() -> None:
    failure = PluginCatalogBuildFailureV1.create(
        stage=CatalogBuildStageV1.SNAPSHOT_FREEZE,
        descriptor=None,
        field_path="failure_001",
        reason_code=MiniQMTPluginReasonCode.REGISTRATION_CONFLICT,
        context={"index": 1},
    )
    failure_payload = {
        "total_failure_count": 2,
        "failures_truncated": False,
        "ordered_failures": [failure.canonical_payload_v1()],
        "omitted_failure_set_sha256": None,
    }
    payload = {
        "schema_version": "plugin_catalog_build_failure_receipt_v1",
        "build_input_sha256": "a" * 64,
        "ordered_descriptor_keys": [],
        **failure_payload,
        "failure_set_sha256": hash_hex_v1("miniqmt_plugin_catalog_failure_set_v1", failure_payload),
    }
    payload["receipt_sha256"] = hash_hex_v1("miniqmt_plugin_catalog_build_failure_receipt_v1", payload)
    payload["ordered_descriptor_keys"] = ()
    payload["ordered_failures"] = (failure,)

    with pytest.raises(ValueError, match="total_failure_count"):
        PluginCatalogBuildFailureReceiptV1.model_validate(payload, strict=True)


def test_catalog_failure_receipt_rejects_empty_writer_and_hash_correct_readback() -> None:
    with pytest.raises(ValueError, match="at least one|non-empty") as create_error:
        PluginCatalogBuildFailureReceiptV1.create(
            build_input_sha256="a" * 64,
            descriptor_keys=(),
            failures=[],
        )
    assert create_error.value.context == {
        "failures_truncated": False,
        "retained_failure_count": 0,
        "total_failure_count": 0,
    }
    json.dumps(create_error.value.context, allow_nan=False)

    failure_payload = {
        "total_failure_count": 0,
        "failures_truncated": False,
        "ordered_failures": [],
        "omitted_failure_set_sha256": None,
    }
    payload = {
        "schema_version": "plugin_catalog_build_failure_receipt_v1",
        "build_input_sha256": "a" * 64,
        "ordered_descriptor_keys": [],
        **failure_payload,
        "failure_set_sha256": hash_hex_v1("miniqmt_plugin_catalog_failure_set_v1", failure_payload),
    }
    payload["receipt_sha256"] = hash_hex_v1("miniqmt_plugin_catalog_build_failure_receipt_v1", payload)
    payload["ordered_descriptor_keys"] = ()
    payload["ordered_failures"] = ()

    with pytest.raises(ValueError, match="at least one|non-empty") as readback_error:
        PluginCatalogBuildFailureReceiptV1.model_validate(payload, strict=True)
    if isinstance(readback_error.value, ValidationError):
        json.dumps(readback_error.value.errors(include_context=False), allow_nan=False)

    failure = PluginCatalogBuildFailureV1.create(
        stage=CatalogBuildStageV1.SNAPSHOT_FREEZE,
        descriptor=None,
        field_path="failure_001",
        reason_code=MiniQMTPluginReasonCode.REGISTRATION_CONFLICT,
        context={"index": 1},
    )
    receipt = PluginCatalogBuildFailureReceiptV1.create(
        build_input_sha256="a" * 64,
        descriptor_keys=(),
        failures=[failure],
    )
    assert receipt.total_failure_count == 1
    assert PluginCatalogBuildFailureReceiptV1.model_validate_json(receipt.model_dump_json()) == receipt


def test_route_evaluator_rejects_stale_gateway_catalog_readback() -> None:
    runtime = _build()
    gateway = _gateway((MarketDataCapabilityV1.L1_BID, MarketDataCapabilityV1.L1_ASK))
    stale = gateway.model_copy(update={"exact_order_id_cancel": False})

    with pytest.raises(ValueError, match="catalog_sha256|gateway capability closure") as error:
        evaluate_plugin_route_compatibility_v1(
            catalog_snapshot=runtime.snapshot,
            plugin_key=runtime.plugin_key_for_new_instance("SNIPER_MINIQMT"),
            gateway_catalog=stale,
        )
    context = getattr(error.value, "context", None)
    assert context["plugin"] == runtime.plugin_key_for_new_instance("SNIPER_MINIQMT").canonical_payload_v1()
    assert context["route"] == gateway.route_id
    assert context["requirement"] == "GATEWAY_CAPABILITY_CATALOG_STRICT_READBACK"
    assert context["gateway_catalog_identity"] == {
        "route_id": gateway.route_id,
        "catalog_sha256": gateway.catalog_sha256,
    }
    json.dumps(context, allow_nan=False)


def test_route_evaluator_rejects_non_catalog_input_with_typed_json_safe_context() -> None:
    runtime = _build()
    plugin_key = runtime.plugin_key_for_new_instance("SNIPER_MINIQMT")

    with pytest.raises(ValueError, match="requires GatewayCapabilityCatalogV1") as error:
        evaluate_plugin_route_compatibility_v1(
            catalog_snapshot=runtime.snapshot,
            plugin_key=plugin_key,
            gateway_catalog={"route_id": "route.invalid"},  # type: ignore[arg-type]
        )

    context = error.value.context
    assert context["plugin"] == plugin_key.canonical_payload_v1()
    assert context["requirement"] == "GATEWAY_CAPABILITY_CATALOG_STRICT_READBACK"
    assert context["expected"] == "GatewayCapabilityCatalogV1"
    json.dumps(context, allow_nan=False)


def test_route_requires_b0_quote_source() -> None:
    runtime = _build()
    plugin_key = runtime.plugin_key_for_new_instance("SNIPER_MINIQMT")
    all_capabilities = (MarketDataCapabilityV1.L1_BID, MarketDataCapabilityV1.L1_ASK)

    wrong_quote = _gateway(all_capabilities, quote_source="LEGACY_OR_SYNTHETIC")
    quote_receipt = evaluate_plugin_route_compatibility_v1(
        catalog_snapshot=runtime.snapshot,
        plugin_key=plugin_key,
        gateway_catalog=wrong_quote,
    )
    assert quote_receipt.status is CompatibilityStatusV1.FAILED
    quote_failure = next(item for item in quote_receipt.ordered_failures if item.field_path == "quote_source")
    assert quote_failure.canonical_payload_v1()["required"] == "B0_QUOTE_V2"
    assert quote_failure.canonical_payload_v1()["supported"] == "LEGACY_OR_SYNTHETIC"
    quote_context = thaw_json_v1(quote_failure.context)
    assert quote_context == {
        "actual": "LEGACY_OR_SYNTHETIC",
        "expected": "B0_QUOTE_V2",
        "gateway_catalog_identity": {
            "catalog_sha256": wrong_quote.catalog_sha256,
            "route_id": wrong_quote.route_id,
        },
        "plugin": {"algo_code": "SNIPER_MINIQMT", "plugin_key": plugin_key.canonical_payload_v1()},
        "requirement": "K1_B0_QUOTE_AUTHORITY",
        "route": wrong_quote.route_id,
    }


@pytest.mark.parametrize("algo_code", ("SNIPER_MINIQMT", "BEST_LIMIT_MINIQMT", "TWAP_LITE_MINIQMT"))
def test_route_requires_exact_cancel_capability_for_cancel_order_plugins(algo_code: str) -> None:
    runtime = _build()
    plugin_key = runtime.plugin_key_for_new_instance(algo_code)
    all_capabilities = (MarketDataCapabilityV1.L1_BID, MarketDataCapabilityV1.L1_ASK)
    no_exact_cancel = _gateway(all_capabilities, exact_order_id_cancel=False)
    cancel_receipt = evaluate_plugin_route_compatibility_v1(
        catalog_snapshot=runtime.snapshot,
        plugin_key=plugin_key,
        gateway_catalog=no_exact_cancel,
    )
    assert cancel_receipt.status is CompatibilityStatusV1.FAILED
    cancel_failure = next(
        item for item in cancel_receipt.ordered_failures if item.field_path == "exact_order_id_cancel"
    )
    assert cancel_failure.canonical_payload_v1()["required"] is True
    assert cancel_failure.canonical_payload_v1()["supported"] is False


def test_route_gateway_writer_readback_identity_and_decision_are_exact() -> None:
    runtime = _build()
    gateway = _gateway((MarketDataCapabilityV1.L1_BID, MarketDataCapabilityV1.L1_ASK))
    readback = GatewayCapabilityCatalogV1.model_validate(gateway.model_dump(mode="python"), strict=True)
    plugin_key = runtime.plugin_key_for_new_instance("BEST_LIMIT_MINIQMT")

    writer_receipt = evaluate_plugin_route_compatibility_v1(
        catalog_snapshot=runtime.snapshot,
        plugin_key=plugin_key,
        gateway_catalog=gateway,
    )
    readback_receipt = evaluate_plugin_route_compatibility_v1(
        catalog_snapshot=runtime.snapshot,
        plugin_key=plugin_key,
        gateway_catalog=readback,
    )

    assert writer_receipt.status is CompatibilityStatusV1.PASSED
    assert writer_receipt.gateway_capability_catalog_sha256 == gateway.catalog_sha256
    assert writer_receipt == readback_receipt


def test_route_receipt_readback_rejects_incomplete_b0_and_exact_cancel_evaluation() -> None:
    runtime = _build()
    gateway = _gateway((MarketDataCapabilityV1.L1_BID, MarketDataCapabilityV1.L1_ASK))
    plugin_key = runtime.plugin_key_for_new_instance("SNIPER_MINIQMT")
    passed = evaluate_plugin_route_compatibility_v1(
        catalog_snapshot=runtime.snapshot,
        plugin_key=plugin_key,
        gateway_catalog=gateway,
    )

    wrong_quote = passed.model_copy(update={"observed_quote_source": "LEGACY_OR_SYNTHETIC"})
    with pytest.raises(ValueError, match="B0 quote authority"):
        type(passed).model_validate(wrong_quote.model_dump(mode="python"), strict=True)

    no_exact_cancel = passed.model_copy(update={"observed_exact_order_id_cancel": False})
    with pytest.raises(ValueError, match="exact order-id cancellation"):
        type(passed).model_validate(no_exact_cancel.model_dump(mode="python"), strict=True)

    failed = evaluate_plugin_route_compatibility_v1(
        catalog_snapshot=runtime.snapshot,
        plugin_key=plugin_key,
        gateway_catalog=_gateway(
            (MarketDataCapabilityV1.L1_BID, MarketDataCapabilityV1.L1_ASK),
            quote_source="LEGACY_OR_SYNTHETIC",
        ),
    )
    stale_failure = failed.model_copy(update={"observed_quote_source": "B0_QUOTE_V2"})
    with pytest.raises(ValueError, match="quote authority fact"):
        type(failed).model_validate(stale_failure.model_dump(mode="python"), strict=True)

    failure = failed.ordered_failures[0]
    incomplete_context = thaw_json_v1(failure.context)
    incomplete_context.pop("requirement")
    incomplete_failure = failure.model_dump(mode="python")
    incomplete_failure["context"] = incomplete_context
    incomplete_failure["context_sha256"] = hash_hex_v1("miniqmt_plugin_route_failure_context_v1", incomplete_context)
    with pytest.raises(ValueError, match="exact evidence key set"):
        type(failure).model_validate(incomplete_failure, strict=True)

    empty_requirement = thaw_json_v1(failure.context)
    empty_requirement["requirement"] = ""
    empty_requirement_failure = failure.model_dump(mode="python")
    empty_requirement_failure["context"] = empty_requirement
    empty_requirement_failure["context_sha256"] = hash_hex_v1(
        "miniqmt_plugin_route_failure_context_v1", empty_requirement
    )
    with pytest.raises(ValueError, match="non-empty strict string"):
        type(failure).model_validate(empty_requirement_failure, strict=True)


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
