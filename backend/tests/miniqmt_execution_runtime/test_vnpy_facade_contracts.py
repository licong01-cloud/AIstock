from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeActiveOrderV1,
    VnpyFacadeAlgorithmCharacterizationReceiptV2,
    VnpyFacadeAlgorithmBindingV1,
    VnpyFacadeCompatibilityStatusV1,
    VnpyFacadeConformanceAuthorityValidationReceiptV2,
    VnpyFacadeContractViewV1,
    VnpyFacadeConformanceFailureV1,
    VnpyFacadeDeterministicInputsV1,
    VnpyFacadeRegistrationDispositionV1,
    VnpyFacadeSourceManifestV1,
    VnpyFacadeSourceRoleV1,
    VnpyFacadeStateValueV1,
    VnpyFacadeUniformDrawV1,
    bound_vnpy_facade_failures_v1,
)
from backend.execution_algos.vnpy_compat.facade_characterization import (
    build_vnpy_facade_contract_v1,
    build_vnpy_facade_isolated_module_bindings_v1,
    build_vnpy_facade_source_manifest_v1,
    build_vnpy_facade_state_mappings_v1,
    build_vnpy_facade_terminal_mappings_v1,
    readback_vnpy_facade_source_manifest_v1,
)
from backend.execution_algos.vnpy_compat.facade_projection import build_vnpy_facade_dto_mappings_v1
from backend.execution_algos.vnpy_compat.locked_surface import PINNED_SOURCE_ROOT
from backend.execution_algos.vnpy_style.plugin_manifests import current_three_manifests_v2
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1


def _market_lineage() -> dict[str, object]:
    return {
        "market_data_id": "market_k4_contract",
        "event_id": "mqrtevt_k4_contract",
        "payload_sha256": "e" * 64,
        "generation": 1,
        "sequence": 1,
        "exchange_time_utc": "2026-07-29T01:30:00Z",
        "session_phase": "CONTINUOUS_AM",
    }


def test_active_order_rejects_missing_or_malformed_native_market_lineage() -> None:
    values = {
        "local_vt_orderid": "local_k4_lineage",
        "broker_order_id": None,
        "command_id": "command_k4_lineage",
        "child_order_id": "child_k4_lineage",
        "symbol": "600000.SH",
        "side": "BUY",
        "price_decimal": "10",
        "requested_quantity": 100,
        "cumulative_quantity": 0,
        "remaining_quantity": 100,
        "status": "COMMAND_PENDING",
        "pending_command_type": "SUBMIT_LIMIT",
        "pending_command_id": "command_k4_lineage",
        "last_order_event_id": None,
        "last_trade_event_id": None,
        "last_command_outcome_event_id": None,
        "last_oms_reconcile_event_id": None,
        "terminal_order_status": None,
        "terminal_observed_cumulative_filled_quantity": None,
    }
    with pytest.raises(ValueError, match="exact market-data lineage"):
        VnpyFacadeActiveOrderV1.create(**values, market_data_lineage={})
    with pytest.raises(ValueError, match="continuous native quote"):
        VnpyFacadeActiveOrderV1.create(
            **values,
            market_data_lineage={**_market_lineage(), "session_phase": "LUNCH_BREAK"},
        )


def test_failed_v2_receipts_cannot_claim_an_empty_failure_set() -> None:
    with pytest.raises(ValueError, match="FAILED characterization receipt V2 requires failures"):
        VnpyFacadeAlgorithmCharacterizationReceiptV2.model_validate(
            {
                "schema_version": "miniqmt_vnpy_facade_algorithm_characterization_receipt_v2",
                "algo_code": "SNIPER_MINIQMT",
                "source_identity_sha256": "1" * 64,
                "facade_source_manifest_sha256": "2" * 64,
                "characterization_requirement_sha256": "3" * 64,
                "canonical_factory_probe_config": {},
                "factory_probe_config_sha256": "4" * 64,
                "facade_contract_sha256": "5" * 64,
                "implementation_binding_set_sha256": "6" * 64,
                "dto_mapping_set_sha256": "7" * 64,
                "state_mapping_set_sha256": "8" * 64,
                "terminal_mapping_set_sha256": "9" * 64,
                "isolated_module_binding_set_sha256": "a" * 64,
                "source_executor_binding_sha256": "b" * 64,
                "source_execution_set_sha256": "c" * 64,
                "ordered_vector_ids": ("vector_one",),
                "vector_set_sha256": "d" * 64,
                "status": VnpyFacadeCompatibilityStatusV1.FAILED,
                "ordered_failures": (),
                "receipt_sha256": "e" * 64,
            },
            strict=True,
        )

    failure = VnpyFacadeConformanceFailureV1.create(
        field_path="authority.vector",
        reason_code="MINIQMT_VNPY_FACADE_SOURCE_EXECUTION_FAILED",
        context={"vector_id": "vector_one"},
    )
    characterization = VnpyFacadeAlgorithmCharacterizationReceiptV2.create(
        algo_code="SNIPER_MINIQMT",
        source_identity_sha256="1" * 64,
        facade_source_manifest_sha256="2" * 64,
        characterization_requirement_sha256="3" * 64,
        canonical_factory_probe_config={},
        factory_probe_config_sha256=hash_hex_v1("miniqmt_vnpy_facade_factory_probe_config_v1", {}),
        facade_contract_sha256="5" * 64,
        implementation_binding_set_sha256="6" * 64,
        dto_mapping_set_sha256="7" * 64,
        state_mapping_set_sha256="8" * 64,
        terminal_mapping_set_sha256="9" * 64,
        isolated_module_binding_set_sha256="a" * 64,
        source_executor_binding_sha256="b" * 64,
        source_execution_set_sha256="c" * 64,
        ordered_vector_ids=("vector_one",),
        vector_set_sha256="d" * 64,
        status=VnpyFacadeCompatibilityStatusV1.FAILED,
        ordered_failures=(failure,),
    )
    validation = VnpyFacadeConformanceAuthorityValidationReceiptV2.create(
        conformance_set_v2_sha256="1" * 64,
        source_executor_binding_sha256="2" * 64,
        ordered_source_execution_set_sha256s=("3" * 64,),
        validation_input_sha256="4" * 64,
        status=VnpyFacadeCompatibilityStatusV1.FAILED,
        ordered_failures=(failure,),
    )
    assert characterization.ordered_failures == (failure,)
    assert validation.ordered_failures == (failure,)
    with pytest.raises(ValueError, match="FAILED conformance authority validation requires failures"):
        VnpyFacadeConformanceAuthorityValidationReceiptV2.model_validate(
            {
                "schema_version": "miniqmt_vnpy_facade_conformance_authority_validation_receipt_v2",
                "conformance_set_v2_sha256": "1" * 64,
                "source_executor_binding_sha256": "2" * 64,
                "ordered_source_execution_set_sha256s": ("3" * 64,),
                "validation_input_sha256": "4" * 64,
                "status": VnpyFacadeCompatibilityStatusV1.FAILED,
                "ordered_failures": (),
                "receipt_sha256": "5" * 64,
            },
            strict=True,
        )


EXPECTED_SOURCES = {
    "vnpy_algotrading/algos/best_limit_algo.py": (
        3560,
        "b35227b932a160c2f786d3202283b61656d9f16631fb42f596a9d376765617e9",
    ),
    "vnpy_algotrading/algos/iceberg_algo.py": (
        3228,
        "9019cd20e4288b1642f7bc5f1508244eb9ccb419a2a888f69040fd9c5c6a2c21",
    ),
    "vnpy_algotrading/algos/sniper_algo.py": (
        2186,
        "fbf84d2c61f8200079fe1f8da3b3412a036e5a7ffb6c601f9e4614ad110c8c76",
    ),
    "vnpy_algotrading/algos/stop_algo.py": (
        2631,
        "18a758b2d86b0704b00ce385f3517061e21dee57178c3abfd10271091e8db090",
    ),
    "vnpy_algotrading/algos/twap_algo.py": (
        2532,
        "aeabb067ef79d48182f357b8d4736f8a90f6a4ecb77bc82506a3244575a6cd0f",
    ),
    "vnpy_core/vnpy/trader/utility.py": (
        32957,
        "9bce3f6e18c84668b0ffadd717f0b6fd4ca2b454dc748dad6572af78c850608d",
    ),
}


def _copy_authority(tmp_path: Path) -> Path:
    target = tmp_path / "pinned_source"
    shutil.copytree(PINNED_SOURCE_ROOT, target)
    return target


def test_real_facade_source_manifest_is_exact_and_readback_stable() -> None:
    manifest = build_vnpy_facade_source_manifest_v1()

    actual = {item.source_path: (item.source_size, item.source_sha256) for item in manifest.ordered_sources}
    assert actual == EXPECTED_SOURCES
    assert tuple(item.algo_code_or_helper_name for item in manifest.ordered_sources) == (
        "ICEBERG",
        "STOP",
        "BEST_LIMIT_MINIQMT",
        "SNIPER_MINIQMT",
        "TWAP_LITE_MINIQMT",
        "round_to",
    )
    assert readback_vnpy_facade_source_manifest_v1(manifest.model_dump(mode="python")) == manifest


@pytest.mark.parametrize(
    ("relative_path", "mutation"),
    [
        ("vnpy_algotrading/algos/iceberg_algo.py", "missing"),
        ("vnpy_algotrading/algos/stop_algo.py", "bytes"),
        ("vnpy_core/vnpy/trader/utility.py", "round_to_body"),
    ],
)
def test_facade_source_authority_drift_fails_loud(
    tmp_path: Path,
    relative_path: str,
    mutation: str,
) -> None:
    source_root = _copy_authority(tmp_path)
    path = source_root / relative_path
    if mutation == "missing":
        path.unlink()
    elif mutation == "bytes":
        path.write_bytes(path.read_bytes() + b"\n")
    else:
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                "int(round(decimal_value / decimal_target))",
                "int(decimal_value / decimal_target)",
            ),
            encoding="utf-8",
            newline="\n",
        )

    with pytest.raises(ValueError, match="MINIQMT_VNPY_FACADE_SOURCE_INVALID"):
        build_vnpy_facade_source_manifest_v1(source_root=source_root)


def test_facade_failure_set_is_bounded_explicit_and_json_safe() -> None:
    failures = tuple(
        VnpyFacadeConformanceFailureV1.create(
            field_path=f"field.{index:03d}",
            reason_code="MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
            context={"index": index},
        )
        for index in range(500)
    )
    bounded = bound_vnpy_facade_failures_v1(failures)

    assert len(bounded) == 256
    assert bounded[-1].field_path == "__failure_set__"
    assert bounded[-1].reason_code == "MINIQMT_VNPY_FACADE_FAILURES_TRUNCATED"
    json.dumps(bounded[-1].model_dump(mode="json"), ensure_ascii=False)


def _assert_hash_drift_rejected(model: object, hash_field: str) -> None:
    payload = model.model_dump(mode="python")  # type: ignore[attr-defined]
    payload[hash_field] = "f" * 64 if payload[hash_field] != "f" * 64 else "e" * 64
    with pytest.raises(ValueError, match="hash mismatch"):
        type(model).model_validate(payload, strict=True)


def test_contract_carrier_hash_readback_rejects_self_consistent_type_drift() -> None:
    manifest = build_vnpy_facade_source_manifest_v1()
    requirements = tuple(item.compatibility_requirement for item in current_three_manifests_v2())
    contract = build_vnpy_facade_contract_v1(compatibility_requirements=requirements)
    implementation_hashes = {
        item.component_name: item.binding_sha256 for item in contract.ordered_implementation_bindings
    }
    isolated = build_vnpy_facade_isolated_module_bindings_v1(
        implementation_binding_sha256_by_component=implementation_hashes
    )
    state_mapping = build_vnpy_facade_state_mappings_v1()[0]
    terminal_mapping = build_vnpy_facade_terminal_mappings_v1()[0]
    dto_mapping = build_vnpy_facade_dto_mappings_v1()[0]
    failure = VnpyFacadeConformanceFailureV1.create(
        field_path="contract.test",
        reason_code="MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
        context={"field": "test"},
    )
    state_value = VnpyFacadeStateValueV1.create(name="count", value=1, value_type="int")
    contract_view = VnpyFacadeContractViewV1.create(
        runtime_id="runtime_k4_contract",
        algo_instance_id="algo_k4_contract",
        symbol="600000.SH",
        exchange_member="SSE",
        gateway_name="qmt",
        min_volume="100",
        volume_increment="100",
        pricetick_decimal="0.01",
        contract_projection_sha256="a" * 64,
        gateway_catalog_sha256="b" * 64,
        route_receipt_sha256="c" * 64,
    )
    active_order = VnpyFacadeActiveOrderV1.create(
        local_vt_orderid="local_k4_contract",
        broker_order_id=None,
        command_id="command_k4_contract",
        child_order_id="child_k4_contract",
        symbol="600000.SH",
        side="BUY",
        price_decimal="10",
        requested_quantity=100,
        cumulative_quantity=0,
        remaining_quantity=100,
        status="COMMAND_PENDING",
        pending_command_type="SUBMIT_LIMIT",
        pending_command_id="command_k4_contract",
        last_order_event_id=None,
        last_trade_event_id=None,
        last_command_outcome_event_id=None,
        last_oms_reconcile_event_id=None,
        terminal_order_status=None,
        terminal_observed_cumulative_filled_quantity=None,
        market_data_lineage=_market_lineage(),
    )
    draw = VnpyFacadeUniformDrawV1.create(ordinal=0, u53_integer=0)
    inputs = VnpyFacadeDeterministicInputsV1.create(ordered_uniform_draws=(draw,))
    algorithm_binding = VnpyFacadeAlgorithmBindingV1.create(
        algo_code="SNIPER_MINIQMT",
        source_identity_sha256="1" * 64,
        class_ref="vnpy_algotrading.algos.sniper_algo:SniperAlgo",
        constructor_signature_sha256="2" * 64,
        constructor_body_sha256="3" * 64,
        state_mapping_set_sha256="4" * 64,
        terminal_mapping_set_sha256="5" * 64,
        characterization_receipt_sha256="6" * 64,
        adapter_contract_sha256="7" * 64,
    )

    for model, hash_field in (
        (manifest.ordered_sources[0], "source_identity_sha256"),
        (manifest, "manifest_sha256"),
        (failure, "context_sha256"),
        (state_value, "value_sha256"),
        (contract_view, "contract_view_sha256"),
        (active_order, "active_order_sha256"),
        (contract.ordered_implementation_bindings[0], "binding_sha256"),
        (contract.ordered_method_contracts[0], "method_contract_sha256"),
        (dto_mapping, "mapping_sha256"),
        (state_mapping, "mapping_sha256"),
        (terminal_mapping, "mapping_sha256"),
        (isolated[0], "binding_sha256"),
        (contract, "facade_contract_sha256"),
        (draw, "draw_sha256"),
        (inputs, "inputs_sha256"),
        (algorithm_binding, "binding_sha256"),
    ):
        _assert_hash_drift_rejected(model, hash_field)


def test_source_manifest_and_simple_state_semantic_conflicts_fail_loud() -> None:
    manifest = build_vnpy_facade_source_manifest_v1()
    helper = next(item for item in manifest.ordered_sources if item.source_role is VnpyFacadeSourceRoleV1.HELPER)
    algorithm = next(item for item in manifest.ordered_sources if item.source_role is VnpyFacadeSourceRoleV1.ALGORITHM)

    with pytest.raises(ValueError, match="helper source"):
        type(helper).create(
            **{
                **helper.canonical_payload_v1(exclude={"schema_version", "source_identity_sha256"}),
                "registration_disposition": VnpyFacadeRegistrationDispositionV1.CHARACTERIZATION_ONLY_K5,
            }
        )
    with pytest.raises(ValueError, match="algorithm source"):
        type(algorithm).create(
            **{
                **algorithm.canonical_payload_v1(exclude={"schema_version", "source_identity_sha256"}),
                "registration_disposition": VnpyFacadeRegistrationDispositionV1.FACADE_HELPER_ONLY,
            }
        )

    payload = manifest.model_dump(mode="python")
    for authorities in (
        (payload["ordered_upstream_authority_sha256"][0],),
        tuple(reversed(payload["ordered_upstream_authority_sha256"])),
    ):
        changed = {**payload, "ordered_upstream_authority_sha256": authorities}
        with pytest.raises(ValueError, match="authorit"):
            VnpyFacadeSourceManifestV1.model_validate(changed, strict=True)
    with pytest.raises(ValueError, match="six canonically ordered"):
        VnpyFacadeSourceManifestV1.model_validate(
            {**payload, "ordered_sources": payload["ordered_sources"][:-1]}, strict=True
        )
    with pytest.raises(ValueError, match="six canonically ordered"):
        VnpyFacadeSourceManifestV1.model_validate(
            {**payload, "ordered_sources": tuple(reversed(payload["ordered_sources"]))}, strict=True
        )
    duplicate_sources = list(manifest.ordered_sources)
    duplicate_sources[1] = duplicate_sources[0]
    duplicate_sources.sort(key=lambda item: item.sort_key_v1())
    with pytest.raises(ValueError, match="duplicate source identities"):
        VnpyFacadeSourceManifestV1.model_validate({**payload, "ordered_sources": tuple(duplicate_sources)}, strict=True)

    requirements = tuple(item.compatibility_requirement for item in current_three_manifests_v2())
    contract = build_vnpy_facade_contract_v1(compatibility_requirements=requirements)
    contract_payload = contract.model_dump(mode="python")
    with pytest.raises(ValueError, match="unique and sorted"):
        type(contract).model_validate(
            {
                **contract_payload,
                "ordered_implementation_bindings": tuple(reversed(contract_payload["ordered_implementation_bindings"])),
            },
            strict=True,
        )
    with pytest.raises(ValueError, match="method contracts must be unique and sorted"):
        type(contract).model_validate(
            {
                **contract_payload,
                "ordered_method_contracts": tuple(reversed(contract_payload["ordered_method_contracts"])),
            },
            strict=True,
        )
    method = next(item for item in contract.ordered_method_contracts if item.ordered_reason_codes)
    method_values = method.canonical_payload_v1(exclude={"schema_version", "method_contract_sha256"})
    with pytest.raises(ValueError, match="duplicates"):
        type(method).create(
            **{
                **method_values,
                "ordered_reason_codes": (
                    method.ordered_reason_codes[0],
                    method.ordered_reason_codes[0],
                ),
            }
        )
    with pytest.raises(TypeError, match="trim-stable"):
        type(method).create(**{**method_values, "ordered_effect_types": (" ",)})

    with pytest.raises(ValueError, match="53-bit"):
        VnpyFacadeUniformDrawV1.create(ordinal=0, u53_integer=2**53)
    draw = VnpyFacadeUniformDrawV1.create(ordinal=1, u53_integer=0)
    with pytest.raises(ValueError, match="contiguous"):
        VnpyFacadeDeterministicInputsV1.create(ordered_uniform_draws=(draw,))

    with pytest.raises(ValueError, match="symbol/exchange"):
        VnpyFacadeContractViewV1.create(
            runtime_id="runtime_k4_contract",
            algo_instance_id="algo_k4_contract",
            symbol="600000.SH",
            exchange_member="SZSE",
            gateway_name="qmt",
            min_volume="100",
            volume_increment="100",
            pricetick_decimal="0.01",
            contract_projection_sha256="a" * 64,
            gateway_catalog_sha256="b" * 64,
            route_receipt_sha256="c" * 64,
        )
    with pytest.raises(ValueError, match="quantity closure"):
        VnpyFacadeActiveOrderV1.create(
            local_vt_orderid="local_k4_contract",
            broker_order_id=None,
            command_id="command_k4_contract",
            child_order_id="child_k4_contract",
            symbol="600000.SH",
            side="BUY",
            price_decimal="10",
            requested_quantity=100,
            cumulative_quantity=1,
            remaining_quantity=100,
            status="COMMAND_PENDING",
            pending_command_type="SUBMIT_LIMIT",
            pending_command_id="command_k4_contract",
            last_order_event_id=None,
            last_trade_event_id=None,
            last_command_outcome_event_id=None,
            last_oms_reconcile_event_id=None,
            terminal_order_status=None,
            terminal_observed_cumulative_filled_quantity=None,
            market_data_lineage=_market_lineage(),
        )


def test_failure_bounding_rejects_duplicate_and_caller_marker() -> None:
    failure = VnpyFacadeConformanceFailureV1.create(
        field_path="contract.test",
        reason_code="MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
        context={"field": "test"},
    )
    with pytest.raises(TypeError, match="tuple"):
        bound_vnpy_facade_failures_v1([failure])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="duplicates"):
        bound_vnpy_facade_failures_v1((failure, failure))
    marker = VnpyFacadeConformanceFailureV1.create(
        field_path="__failure_set__",
        reason_code="MINIQMT_VNPY_FACADE_FAILURES_TRUNCATED",
        context={"omitted_count": 1, "omitted_failure_set_sha256": "a" * 64},
    )
    with pytest.raises(ValueError, match="caller-supplied"):
        bound_vnpy_facade_failures_v1((marker,))
