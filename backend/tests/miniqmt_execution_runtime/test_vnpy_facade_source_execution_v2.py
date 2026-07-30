from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1, thaw_json_v1
from backend.execution_algos.vnpy_compat.facade_characterization import (
    build_vnpy_facade_characterization_requirements_v1,
    build_vnpy_facade_contract_v1,
    build_vnpy_facade_source_manifest_v1,
    readback_vnpy_facade_characterization_vector_artifact_v2,
)

from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeCompatibilityStatusV1,
    VnpyFacadeAlgorithmBindingV2,
    VnpyFacadeAlgorithmCharacterizationReceiptV2,
    VnpyFacadeConformanceFailureV1,
    VnpyFacadeDeterministicInputsV1,
    VnpyFacadeExecutedVectorResultV1,
    VnpyFacadeSourceExecutionSetV1,
    VnpyFacadeSourceExecutorBindingV1,
    VnpyFacadeSourceStateEnvelopeV1,
    VnpyFacadeStateValueV1,
    VnpyFacadeTraceCallV1,
    VnpyFacadeContractError,
)
from backend.execution_algos.vnpy_compat.facade_source_execution import (
    _active_orders_v1,
    _callback_v1,
    _decode_state_value_v1,
    _failed_result_v1,
    _safe_exception_evidence_v1,
    _state_value_v1,
    build_vnpy_facade_source_executor_binding_v1,
    execute_vnpy_facade_source_vectors_v1,
    readback_vnpy_facade_source_executor_binding_v1,
)
from backend.execution_algos.vnpy_compat.facade import VnpyFacadeEffectCollectorV1, VnpyFacadeTraceCollectorV2
from backend.execution_algos.vnpy_compat.facade_projection import Status
from backend.execution_algos.vnpy_style.plugin_manifests import current_three_manifests_v3
from backend.services.miniqmt_execution_runtime.plugin_contracts import EventTypeV2, algo_transition_id_v1
from backend.tests.miniqmt_execution_runtime.test_kernel_creation import _catalog


def _trace_call(*, ordinal: int = 0, method_name: str = "get_tick") -> VnpyFacadeTraceCallV1:
    return VnpyFacadeTraceCallV1.create(
        ordinal=ordinal,
        method_name=method_name,
        normalized_arguments={"algo_instance_id": "algo_k4_v2"},
        return_disposition="NONE",
        normalized_return_or_null=None,
        ordered_diagnostic_reason_codes=("MINIQMT_VNPY_FACADE_TICK_UNAVAILABLE",),
    )


def _state() -> VnpyFacadeSourceStateEnvelopeV1:
    return VnpyFacadeSourceStateEnvelopeV1.create(
        runtime_id="runtime_k4_v2",
        algo_instance_id="algo_k4_v2",
        algo_code="SNIPER_MINIQMT",
        source_identity_sha256="a" * 64,
        manifest_view_sha256="b" * 64,
        algo_name="algo_k4_v2",
        symbol="600000.SH",
        direction_member="LONG",
        offset_member="NONE",
        limit_price_decimal="10",
        target_volume_decimal="100",
        status_member="RUNNING",
        traded_volume_decimal="0",
        traded_price_decimal="0",
        contract_projection={
            "gateway_name": "minqmt_sim",
            "min_volume": "100",
            "volume_increment": "100",
            "pricetick_decimal": "0.01",
        },
        ordered_active_orders=(),
        ordered_parameters=(),
        ordered_variables=(),
        state_mapping_set_sha256="f" * 64,
    )


def test_v2_trace_call_is_strict_hash_closed_and_immutable() -> None:
    call = _trace_call()

    assert call.ordinal == 0
    assert call.method_name == "get_tick"
    assert len(call.call_sha256) == 64
    with pytest.raises(TypeError):
        call.normalized_arguments["algo_instance_id"] = "drift"  # type: ignore[index]
    supplied = call.model_dump(mode="python")
    supplied["call_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="trace call hash"):
        VnpyFacadeTraceCallV1.model_validate(supplied, strict=True)


def test_static_artifact_callback_quantities_close_to_exact_child_authority() -> None:
    artifact = readback_vnpy_facade_characterization_vector_artifact_v2().artifact

    for vector in artifact.ordered_vectors:
        event = vector.runtime_event_or_null
        if event is None or event.event_type not in {EventTypeV2.ORDER, EventTypeV2.TRADE}:
            continue
        payload = thaw_json_v1(event.payload)
        matches = tuple(
            item for item in vector.ordered_active_mappings if item.local_vt_orderid == payload["local_vt_orderid"]
        )
        assert len(matches) == 1, vector.vector_id
        requested = matches[0].requested_quantity
        if event.event_type is EventTypeV2.ORDER:
            cumulative = payload["observed_cumulative_filled_quantity"]
            remaining = payload["observed_remaining_quantity"]
            if cumulative is not None:
                assert cumulative + remaining == requested, vector.vector_id
        else:
            assert payload["trade_quantity"] <= requested, vector.vector_id

        for active in vector.expected_after_state.ordered_active_orders:
            assert active.cumulative_quantity <= active.requested_quantity, vector.vector_id
            assert active.cumulative_quantity + active.remaining_quantity == active.requested_quantity


def test_v2_execution_set_requires_complete_ordered_results_and_no_failure() -> None:
    result = VnpyFacadeExecutedVectorResultV1.create(
        vector_id="vector_k4_v2_000",
        vector_sha256="1" * 64,
        scenario_id="scenario_k4_v2",
        step_ordinal=0,
        source_executor_binding_sha256="2" * 64,
        source_identity_sha256="3" * 64,
        invocation_status="COMPLETED",
        actual_ordered_facade_calls=(_trace_call(),),
        actual_ordered_effects=(),
        actual_after_state_or_null=_state(),
        actual_terminal_outcome=None,
        consumed_deterministic_inputs=VnpyFacadeDeterministicInputsV1.create(ordered_uniform_draws=()),
        ordered_execution_failures=(),
    )
    execution_set = VnpyFacadeSourceExecutionSetV1.create(
        algo_code="SNIPER_MINIQMT",
        characterization_requirement_sha256="6" * 64,
        source_executor_binding_sha256="2" * 64,
        facade_source_manifest_sha256="7" * 64,
        facade_contract_sha256="8" * 64,
        vector_set_sha256="9" * 64,
        ordered_results=(result,),
        ordered_failures=(),
        status=VnpyFacadeCompatibilityStatusV1.PASSED,
    )

    assert execution_set.ordered_results == (result,)
    assert execution_set.status is VnpyFacadeCompatibilityStatusV1.PASSED

    failure = VnpyFacadeConformanceFailureV1.create(
        field_path="ordered_results[0]",
        reason_code="MINIQMT_VNPY_FACADE_SOURCE_EXECUTION_FAILED",
        context={"vector_id": result.vector_id},
    )
    with pytest.raises(ValueError, match="PASSED source execution set"):
        VnpyFacadeSourceExecutionSetV1.create(
            algo_code="SNIPER_MINIQMT",
            characterization_requirement_sha256="6" * 64,
            source_executor_binding_sha256="2" * 64,
            facade_source_manifest_sha256="7" * 64,
            facade_contract_sha256="8" * 64,
            vector_set_sha256="9" * 64,
            ordered_results=(result,),
            ordered_failures=(failure,),
            status=VnpyFacadeCompatibilityStatusV1.PASSED,
        )


def test_v2_result_rejects_failed_without_failure_and_non_contiguous_call_ordinals() -> None:
    with pytest.raises(ValueError, match="FAILED execution result"):
        VnpyFacadeExecutedVectorResultV1.create(
            vector_id="vector_k4_v2_failed",
            vector_sha256="1" * 64,
            scenario_id="scenario_k4_v2",
            step_ordinal=0,
            source_executor_binding_sha256="2" * 64,
            source_identity_sha256="3" * 64,
            invocation_status="FAILED",
            actual_ordered_facade_calls=(),
            actual_ordered_effects=(),
            actual_after_state_or_null=None,
            actual_terminal_outcome=None,
            consumed_deterministic_inputs=VnpyFacadeDeterministicInputsV1.create(ordered_uniform_draws=()),
            ordered_execution_failures=(),
        )

    with pytest.raises(ValueError, match="contiguous"):
        VnpyFacadeExecutedVectorResultV1.create(
            vector_id="vector_k4_v2_gap",
            vector_sha256="1" * 64,
            scenario_id="scenario_k4_v2",
            step_ordinal=0,
            source_executor_binding_sha256="2" * 64,
            source_identity_sha256="3" * 64,
            invocation_status="COMPLETED",
            actual_ordered_facade_calls=(_trace_call(ordinal=1),),
            actual_ordered_effects=(),
            actual_after_state_or_null=_state(),
            actual_terminal_outcome=None,
            consumed_deterministic_inputs=VnpyFacadeDeterministicInputsV1.create(ordered_uniform_draws=()),
            ordered_execution_failures=(),
        )


def test_v2_source_binding_characterization_and_algorithm_binding_are_hash_closed() -> None:
    executor = VnpyFacadeSourceExecutorBindingV1.create(
        executor_ref="backend.execution_algos.vnpy_compat.facade_source_execution:execute_vnpy_facade_vectors_v1",
        executor_signature_sha256="1" * 64,
        executor_source_sha256="2" * 64,
        facade_source_manifest_sha256="3" * 64,
        facade_contract_sha256="4" * 64,
        implementation_binding_set_sha256="5" * 64,
        isolated_module_binding_set_sha256="6" * 64,
        dto_mapping_set_sha256="7" * 64,
        state_mapping_set_sha256="8" * 64,
        terminal_mapping_set_sha256="9" * 64,
        vector_artifact_sha256="4" * 64,
        vector_artifact_file_sha256="5" * 64,
        supported_algo_codes=(
            "BEST_LIMIT_MINIQMT",
            "ICEBERG",
            "SNIPER_MINIQMT",
            "STOP",
            "TWAP_LITE_MINIQMT",
        ),
    )
    result = VnpyFacadeExecutedVectorResultV1.create(
        vector_id="vector_k4_v2_000",
        vector_sha256="a" * 64,
        scenario_id="scenario_k4_v2",
        step_ordinal=0,
        source_executor_binding_sha256=executor.binding_sha256,
        source_identity_sha256="b" * 64,
        invocation_status="COMPLETED",
        actual_ordered_facade_calls=(_trace_call(),),
        actual_ordered_effects=(),
        actual_after_state_or_null=_state(),
        actual_terminal_outcome=None,
        consumed_deterministic_inputs=VnpyFacadeDeterministicInputsV1.create(ordered_uniform_draws=()),
        ordered_execution_failures=(),
    )
    execution_set = VnpyFacadeSourceExecutionSetV1.create(
        algo_code="SNIPER_MINIQMT",
        characterization_requirement_sha256="c" * 64,
        source_executor_binding_sha256=executor.binding_sha256,
        facade_source_manifest_sha256="3" * 64,
        facade_contract_sha256="4" * 64,
        vector_set_sha256="d" * 64,
        ordered_results=(result,),
        ordered_failures=(),
        status=VnpyFacadeCompatibilityStatusV1.PASSED,
    )
    receipt = VnpyFacadeAlgorithmCharacterizationReceiptV2.create(
        algo_code="SNIPER_MINIQMT",
        source_identity_sha256="b" * 64,
        facade_source_manifest_sha256="3" * 64,
        characterization_requirement_sha256="c" * 64,
        canonical_factory_probe_config={"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
        factory_probe_config_sha256=hash_hex_v1(
            "miniqmt_vnpy_facade_factory_probe_config_v1",
            {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
        ),
        facade_contract_sha256="4" * 64,
        implementation_binding_set_sha256="5" * 64,
        dto_mapping_set_sha256="7" * 64,
        state_mapping_set_sha256="8" * 64,
        terminal_mapping_set_sha256="9" * 64,
        isolated_module_binding_set_sha256="6" * 64,
        source_executor_binding_sha256=executor.binding_sha256,
        source_execution_set_sha256=execution_set.execution_set_sha256,
        ordered_vector_ids=(result.vector_id,),
        vector_set_sha256=execution_set.vector_set_sha256,
        status=VnpyFacadeCompatibilityStatusV1.PASSED,
        ordered_failures=(),
    )
    binding = VnpyFacadeAlgorithmBindingV2.create(
        algo_code="SNIPER_MINIQMT",
        source_identity_sha256="b" * 64,
        class_ref="vnpy_algotrading.algos.sniper_algo:SniperAlgo",
        constructor_signature_sha256="f" * 64,
        constructor_body_sha256="0" * 64,
        state_mapping_set_sha256="8" * 64,
        terminal_mapping_set_sha256="9" * 64,
        characterization_receipt_sha256=receipt.receipt_sha256,
        adapter_contract_sha256="4" * 64,
        source_executor_binding_sha256=executor.binding_sha256,
        source_execution_set_sha256=execution_set.execution_set_sha256,
    )

    assert receipt.status is VnpyFacadeCompatibilityStatusV1.PASSED
    assert binding.characterization_receipt_sha256 == receipt.receipt_sha256
    assert binding.source_execution_set_sha256 == execution_set.execution_set_sha256

    forged = receipt.model_dump(mode="python")
    forged["source_execution_set_sha256"] = "1" * 64
    with pytest.raises(ValueError, match="characterization receipt hash"):
        VnpyFacadeAlgorithmCharacterizationReceiptV2.model_validate(forged, strict=True)


def test_static_artifact_sniper_source_executor_matches_full_authoritative_trace() -> None:
    manifests = current_three_manifests_v3()
    source_manifest = build_vnpy_facade_source_manifest_v1()
    facade_contract = build_vnpy_facade_contract_v1(
        compatibility_requirements=tuple(item.compatibility_requirement for item in manifests)
    )
    requirements = build_vnpy_facade_characterization_requirements_v1(
        catalog_runtime=_catalog(), source_manifest=source_manifest
    )
    artifact = readback_vnpy_facade_characterization_vector_artifact_v2()
    binding = build_vnpy_facade_source_executor_binding_v1(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        vector_artifact_sha256=artifact.artifact.artifact_sha256,
        vector_artifact_file_sha256=artifact.canonical_lf_file_sha256,
    )
    vectors = tuple(item for item in artifact.artifact.ordered_vectors if item.algo_code == "SNIPER_MINIQMT")
    requirement = next(item for item in requirements if item.algo_code == "SNIPER_MINIQMT")

    execution_set = execute_vnpy_facade_source_vectors_v1(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        requirements=(requirement,),
        ordered_vectors=vectors,
        source_executor_binding=binding,
    )[0]

    assert execution_set.status is VnpyFacadeCompatibilityStatusV1.PASSED
    assert len(execution_set.ordered_results) == len(vectors)
    assert tuple(item.vector_id for item in execution_set.ordered_results) == tuple(item.vector_id for item in vectors)
    assert all(item.invocation_status == "COMPLETED" for item in execution_set.ordered_results)
    assert any(item.actual_ordered_effects for item in execution_set.ordered_results)
    assert {
        call.method_name for result in execution_set.ordered_results for call in result.actual_ordered_facade_calls
    }.issuperset({"__init__", "send_order", "cancel_order", "get_tick"})


def test_static_artifact_executes_all_five_algorithms_through_the_public_source_authority() -> None:
    source_manifest, facade_contract, requirements, artifact, binding = _real_source_inputs()

    execution_sets = execute_vnpy_facade_source_vectors_v1(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        requirements=requirements,
        ordered_vectors=artifact.artifact.ordered_vectors,
        source_executor_binding=binding,
    )

    assert tuple(item.algo_code for item in execution_sets) == (
        "BEST_LIMIT_MINIQMT",
        "ICEBERG",
        "SNIPER_MINIQMT",
        "STOP",
        "TWAP_LITE_MINIQMT",
    )
    assert all(item.status is VnpyFacadeCompatibilityStatusV1.PASSED for item in execution_sets)
    assert sum(len(item.ordered_results) for item in execution_sets) == len(artifact.artifact.ordered_vectors)


def test_source_active_order_projection_rejects_mapping_identity_and_mutable_fact_drift() -> None:
    artifact = readback_vnpy_facade_characterization_vector_artifact_v2().artifact
    vector = next(
        item
        for item in artifact.ordered_vectors
        if item.runtime_event_or_null is not None
        and item.runtime_event_or_null.event_type is EventTypeV2.TICK
        and item.before_state_or_null is not None
        and item.before_state_or_null.ordered_active_orders
        and item.ordered_active_mappings
    )
    assert vector.before_state_or_null is not None
    assert vector.runtime_event_or_null is not None
    mapping = vector.ordered_active_mappings[0]
    collector = VnpyFacadeEffectCollectorV1.create(
        vector.deterministic_context,
        mapping.parent_intent_id,
        algo_transition_id_v1(
            delivery_id=vector.deterministic_context.delivery_id,
            event_id=vector.deterministic_context.event_id,
            runtime_id=vector.deterministic_context.runtime_id,
            algo_instance_id=vector.deterministic_context.algo_instance_id,
            transition_sequence=vector.deterministic_context.transition_sequence,
        ),
    )
    common = {
        "algorithm": SimpleNamespace(active_orders={}),
        "before": vector.before_state_or_null,
        "effect_collector": collector,
        "event": vector.runtime_event_or_null,
        "market_data_lineage_or_none": None,
    }
    with pytest.raises(ValueError, match="no predecessor"):
        _active_orders_v1(
            active_mappings=(mapping.model_copy(update={"local_vt_orderid": "missing_local"}),),
            **common,
        )
    with pytest.raises(ValueError, match="conflicts with predecessor"):
        _active_orders_v1(
            active_mappings=(mapping.model_copy(update={"command_id": "drifted_command"}),),
            **common,
        )
    with pytest.raises(ValueError, match="mutable facts drifted"):
        _active_orders_v1(
            active_mappings=(mapping.model_copy(update={"broker_order_id": "drifted_broker"}),),
            **common,
        )

    assert _state_value_v1(1e20) == "100000000000000000000"
    _callback_v1(
        algorithm=SimpleNamespace(),
        event=vector.runtime_event_or_null,
        facade=SimpleNamespace(get_tick=lambda _algorithm: None),
        before=vector.before_state_or_null,
        trace=SimpleNamespace(),
    )


def _real_source_inputs():
    manifests = current_three_manifests_v3()
    source_manifest = build_vnpy_facade_source_manifest_v1()
    facade_contract = build_vnpy_facade_contract_v1(
        compatibility_requirements=tuple(item.compatibility_requirement for item in manifests)
    )
    requirements = build_vnpy_facade_characterization_requirements_v1(
        catalog_runtime=_catalog(), source_manifest=source_manifest
    )
    artifact = readback_vnpy_facade_characterization_vector_artifact_v2()
    binding = build_vnpy_facade_source_executor_binding_v1(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        vector_artifact_sha256=artifact.artifact.artifact_sha256,
        vector_artifact_file_sha256=artifact.canonical_lf_file_sha256,
    )
    return source_manifest, facade_contract, requirements, artifact, binding


def test_source_executor_binding_and_requirement_inputs_fail_loud_without_fallback() -> None:
    source_manifest, facade_contract, requirements, artifact, binding = _real_source_inputs()

    with pytest.raises(TypeError, match="source_manifest"):
        build_vnpy_facade_source_executor_binding_v1(
            source_manifest=object(),  # type: ignore[arg-type]
            facade_contract=facade_contract,
            vector_artifact_sha256=artifact.artifact.artifact_sha256,
            vector_artifact_file_sha256=artifact.canonical_lf_file_sha256,
        )
    with pytest.raises(TypeError, match="facade_contract"):
        build_vnpy_facade_source_executor_binding_v1(
            source_manifest=source_manifest,
            facade_contract=object(),  # type: ignore[arg-type]
            vector_artifact_sha256=artifact.artifact.artifact_sha256,
            vector_artifact_file_sha256=artifact.canonical_lf_file_sha256,
        )
    forged_values = binding.model_dump(mode="python", exclude={"schema_version", "binding_sha256"})
    forged_values["executor_source_sha256"] = "0" * 64
    forged = VnpyFacadeSourceExecutorBindingV1.create(**forged_values)
    with pytest.raises(VnpyFacadeContractError, match="live callable/source authority"):
        readback_vnpy_facade_source_executor_binding_v1(
            forged,
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            vector_artifact_sha256=artifact.artifact.artifact_sha256,
            vector_artifact_file_sha256=artifact.canonical_lf_file_sha256,
        )
    with pytest.raises(TypeError, match="source_executor_binding"):
        execute_vnpy_facade_source_vectors_v1(
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            requirements=requirements,
            ordered_vectors=artifact.artifact.ordered_vectors,
            source_executor_binding=object(),  # type: ignore[arg-type]
        )
    with pytest.raises(VnpyFacadeContractError, match="binding conflicts"):
        execute_vnpy_facade_source_vectors_v1(
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            requirements=requirements,
            ordered_vectors=artifact.artifact.ordered_vectors,
            source_executor_binding=binding.model_copy(update={"facade_contract_sha256": "0" * 64}),
        )
    for invalid_requirements in (
        (),
        (requirements[0], requirements[0]),
        (requirements[0].model_copy(update={"algo_code": "UNSUPPORTED_ALGO"}),),
    ):
        with pytest.raises(VnpyFacadeContractError, match="unique supported requirement"):
            execute_vnpy_facade_source_vectors_v1(
                source_manifest=source_manifest,
                facade_contract=facade_contract,
                requirements=invalid_requirements,
                ordered_vectors=artifact.artifact.ordered_vectors,
                source_executor_binding=binding,
            )


def test_source_executor_retains_missing_predecessor_empty_vector_and_expected_drift_failures() -> None:
    source_manifest, facade_contract, requirements, artifact, binding = _real_source_inputs()
    sniper_requirement = next(item for item in requirements if item.algo_code == "SNIPER_MINIQMT")
    sniper_vectors = tuple(item for item in artifact.artifact.ordered_vectors if item.algo_code == "SNIPER_MINIQMT")

    empty = execute_vnpy_facade_source_vectors_v1(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        requirements=(sniper_requirement,),
        ordered_vectors=(),
        source_executor_binding=binding,
    )[0]
    transition_only = execute_vnpy_facade_source_vectors_v1(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        requirements=(sniper_requirement,),
        ordered_vectors=(next(item for item in sniper_vectors if item.invocation_phase == "TRANSITION"),),
        source_executor_binding=binding,
    )[0]
    initial_vectors = tuple(item for item in sniper_vectors if item.invocation_phase == "INITIALIZE")
    original = initial_vectors[0]
    drift_values = original.model_dump(
        mode="python",
        exclude={"schema_version", "vector_sha256", "expected_after_state"},
    )
    drifted_vector = type(original).create(
        **drift_values,
        expected_after_state=initial_vectors[-1].expected_after_state,
    )
    drifted = execute_vnpy_facade_source_vectors_v1(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        requirements=(sniper_requirement,),
        ordered_vectors=(drifted_vector,),
        source_executor_binding=binding,
    )[0]

    assert empty.status is VnpyFacadeCompatibilityStatusV1.FAILED
    assert transition_only.status is VnpyFacadeCompatibilityStatusV1.FAILED
    assert transition_only.ordered_results[0].invocation_status == "FAILED"
    assert drifted.status is VnpyFacadeCompatibilityStatusV1.FAILED
    assert drifted.ordered_results[0].invocation_status == "COMPLETED"
    assert "after_state" in thaw_json_v1(drifted.ordered_failures[0].context)["mismatched_fields"]


def test_source_executor_missing_repo_owned_source_and_state_codecs_are_explicit(
    tmp_path: Path,
) -> None:
    source_manifest, facade_contract, requirements, artifact, binding = _real_source_inputs()
    with pytest.raises(VnpyFacadeContractError):
        execute_vnpy_facade_source_vectors_v1(
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            requirements=(requirements[0],),
            ordered_vectors=tuple(
                item for item in artifact.artifact.ordered_vectors if item.algo_code == requirements[0].algo_code
            ),
            source_executor_binding=binding,
            source_root=tmp_path,
        )

    assert _state_value_v1(Status.NOTTRADED)["member"] == "NOTTRADED"
    assert _state_value_v1(None) is None
    assert _state_value_v1(True) is True
    assert _state_value_v1(3) == 3
    assert _state_value_v1(-0.0) == "0"
    with pytest.raises(ValueError, match="non-finite"):
        _state_value_v1(float("inf"))
    with pytest.raises(TypeError, match="unsupported"):
        _state_value_v1(object())

    enum_value = VnpyFacadeStateValueV1.create(
        name="status",
        value={"enum_owner": "Status", "member": "NOTTRADED", "pinned_value": "未成交"},
        value_type="Status",
    )
    assert _decode_state_value_v1(enum_value) is Status.NOTTRADED
    float_value = VnpyFacadeStateValueV1.create(name="price", value="1.5", value_type="float")
    assert _decode_state_value_v1(float_value) == 1.5
    with pytest.raises(ValueError, match="enum owner"):
        _decode_state_value_v1(
            VnpyFacadeStateValueV1.create(
                name="direction",
                value={"enum_owner": "Direction", "member": "LONG", "pinned_value": "多"},
                value_type="Direction",
            )
        )
    with pytest.raises(TypeError, match="strict integer"):
        _decode_state_value_v1(VnpyFacadeStateValueV1.create(name="count", value="1", value_type="int"))

    class _BrokenMessage(RuntimeError):
        def __str__(self) -> str:
            raise RuntimeError("render failed")

    evidence = _safe_exception_evidence_v1(_BrokenMessage())
    assert evidence["exception_message"] == "<_BrokenMessage: unrenderable>"
    assert evidence["message_render_error_type"].endswith("RuntimeError")
    assert evidence["message_truncated"] is False
    assert evidence["omitted_message_sha256"] is None


def test_source_failure_evidence_is_path_safe_bounded_and_hash_closes_omitted_text() -> None:
    root = str(Path(__file__).resolve().parents[3])
    message = "x" * 2030 + root + "/private.py" + "z" * 3000
    evidence = _safe_exception_evidence_v1(RuntimeError(message))

    assert root not in evidence["exception_message"]
    assert evidence["message_truncated"] is True
    assert evidence["observed_message_chars"] > len(evidence["exception_message"])
    assert len(evidence["omitted_message_sha256"]) == 64


def test_trace_collector_preserves_primary_failure_when_reason_code_property_breaks() -> None:
    artifact = readback_vnpy_facade_characterization_vector_artifact_v2().artifact
    vector = artifact.ordered_vectors[0]
    assert vector.start_context_or_null is not None
    effects = VnpyFacadeEffectCollectorV1.create(
        vector.deterministic_context,
        vector.start_context_or_null.parent_intent_id,
        "transition_trace_primary_failure",
    )
    trace = VnpyFacadeTraceCollectorV2(vector_id=vector.vector_id, effect_collector=effects)

    class _PrimaryFailure(RuntimeError):
        @property
        def reason_code(self) -> str:
            raise LookupError("secondary reason renderer failed")

    def operation() -> None:
        raise _PrimaryFailure("primary failure")

    with pytest.raises(_PrimaryFailure, match="primary failure"):
        trace.invoke_v1(method_name="write_log", normalized_arguments={"msg": "x"}, operation=operation)

    assert len(trace.ordered_calls) == 1
    assert trace.ordered_calls[0].return_disposition == "RAISED"
    assert (
        thaw_json_v1(trace.ordered_calls[0].normalized_return_or_null)["reason_code"]
        == "MINIQMT_VNPY_FACADE_SOURCE_EXECUTION_FAILED"
    )


def test_failed_vector_result_preserves_primary_reason_and_secondary_trace_failure() -> None:
    artifact = readback_vnpy_facade_characterization_vector_artifact_v2()
    vector = artifact.artifact.ordered_vectors[0]
    source_manifest = build_vnpy_facade_source_manifest_v1()
    facade_contract = build_vnpy_facade_contract_v1(
        compatibility_requirements=tuple(item.compatibility_requirement for item in current_three_manifests_v3())
    )
    binding = build_vnpy_facade_source_executor_binding_v1(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        vector_artifact_sha256=artifact.artifact.artifact_sha256,
        vector_artifact_file_sha256=artifact.canonical_lf_file_sha256,
    )

    class _BrokenReason(RuntimeError):
        @property
        def reason_code(self) -> str:
            raise LookupError("secondary reason renderer failed")

    result = _failed_result_v1(
        vector=vector,
        source_executor_binding=binding,
        source_identity_sha256="a" * 64,
        exc=_BrokenReason("primary failure"),
        trace_snapshot_error=RuntimeError("trace snapshot failure"),
    )

    assert result.invocation_status == "FAILED"
    failure = result.ordered_execution_failures[0]
    assert failure.reason_code == "MINIQMT_VNPY_FACADE_SOURCE_EXECUTION_FAILED"
    assert thaw_json_v1(failure.context)["trace_snapshot_failure"]["exception_message"] == "trace snapshot failure"
