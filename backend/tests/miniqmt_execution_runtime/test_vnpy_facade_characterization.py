from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from backend.execution_algos.vnpy_compat.facade_adapter import (
    state_mapping_set_sha256_v1,
    terminal_mapping_set_sha256_v1,
)
from backend.execution_algos.vnpy_compat.facade_characterization import (
    VnpyFacadeDeterministicUniformV1,
    build_vnpy_facade_algorithm_bindings_v1,
    build_vnpy_facade_characterization_receipt_v1,
    build_vnpy_facade_conformance_set_v1,
    build_vnpy_facade_contract_v1,
    build_vnpy_facade_implementation_bindings_v1,
    build_vnpy_facade_isolated_module_bindings_v1,
    build_vnpy_facade_source_manifest_v1,
    build_vnpy_facade_state_mappings_v1,
    build_vnpy_facade_terminal_mappings_v1,
    load_pinned_vnpy_algorithm_classes_v1,
    readback_vnpy_facade_contract_v1,
    readback_vnpy_facade_characterization_receipt_v1,
    readback_vnpy_facade_implementation_bindings_v1,
    readback_vnpy_facade_state_mappings_v1,
    readback_vnpy_facade_terminal_mappings_v1,
)
from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeCharacterizationRequirementV1,
    VnpyFacadeCharacterizationVectorV1,
    VnpyFacadeCompatibilityStatusV1,
    VnpyFacadeContractError,
    VnpyFacadeDeterministicInputsV1,
    VnpyFacadeImplementationBindingV1,
    VnpyFacadeStateFieldMappingV1,
    VnpyFacadeUniformDrawV1,
)
from backend.execution_algos.vnpy_compat.receipts import build_current_three_compatibility_receipts_v1
from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_creation_bindings_v1,
    current_three_descriptors_v2,
    current_three_manifests_v2,
    current_three_process_bindings_v2,
)
from backend.execution_algos.vnpy_compat.locked_surface import PINNED_SOURCE_ROOT
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1, thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_registry import build_plugin_catalog_v2


def test_checked_in_facade_manifest_matches_live_writer() -> None:
    checked_in = json.loads((PINNED_SOURCE_ROOT / "facade_source_manifest.json").read_text(encoding="utf-8"))
    assert checked_in == build_vnpy_facade_source_manifest_v1().model_dump(mode="json")


def test_characterization_public_readbacks_reject_wrong_carrier_shapes(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="pathlib.Path"):
        build_vnpy_facade_source_manifest_v1(source_root=str(PINNED_SOURCE_ROOT))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="source root"):
        build_vnpy_facade_source_manifest_v1(source_root=tmp_path / "missing")
    with pytest.raises(TypeError, match="tuple or JSON list"):
        readback_vnpy_facade_state_mappings_v1({})
    with pytest.raises(TypeError, match="tuple or JSON list"):
        readback_vnpy_facade_terminal_mappings_v1({})
    with pytest.raises(TypeError, match="tuple or JSON list"):
        readback_vnpy_facade_implementation_bindings_v1({})


def test_isolated_loader_uses_exact_five_classes_without_sys_modules_residue() -> None:
    before = {name for name in sys.modules if name.startswith("vnpy")}
    loaded = load_pinned_vnpy_algorithm_classes_v1()
    after = {name for name in sys.modules if name.startswith("vnpy")}

    assert tuple(sorted(loaded)) == (
        "BEST_LIMIT_MINIQMT",
        "ICEBERG",
        "SNIPER_MINIQMT",
        "STOP",
        "TWAP_LITE_MINIQMT",
    )
    assert tuple(loaded[name].__name__ for name in sorted(loaded)) == (
        "BestLimitAlgo",
        "IcebergAlgo",
        "SniperAlgo",
        "StopAlgo",
        "TwapAlgo",
    )
    assert after == before


def test_state_and_terminal_mappings_are_ast_derived_and_strict_readback() -> None:
    state = build_vnpy_facade_state_mappings_v1()
    terminal = build_vnpy_facade_terminal_mappings_v1()

    assert len(state) == 72
    assert len(terminal) == 6
    assert len(state_mapping_set_sha256_v1(state)) == 64
    assert len(terminal_mapping_set_sha256_v1(terminal)) == 64
    assert readback_vnpy_facade_state_mappings_v1([item.model_dump(mode="python") for item in state]) == state
    assert readback_vnpy_facade_terminal_mappings_v1([item.model_dump(mode="python") for item in terminal]) == terminal


def test_hash_correct_state_mapping_drift_is_rejected_by_live_ast_readback() -> None:
    state = build_vnpy_facade_state_mappings_v1()
    changed = VnpyFacadeStateFieldMappingV1.create(
        **{
            **state[0].canonical_payload_v1(exclude={"schema_version", "mapping_sha256"}),
            "value_type": "forged",
        }
    )
    supplied = (changed, *state[1:])

    with pytest.raises(ValueError, match="STATE_MAPPING_INVALID"):
        readback_vnpy_facade_state_mappings_v1([item.model_dump(mode="python") for item in supplied])


def test_deterministic_uniform_consumes_exact_u53_and_retains_trace() -> None:
    inputs = VnpyFacadeDeterministicInputsV1.create(
        ordered_uniform_draws=(VnpyFacadeUniformDrawV1.create(ordinal=0, u53_integer=2**52),)
    )
    uniform = VnpyFacadeDeterministicUniformV1(inputs)

    assert uniform(100.0, 200.0) == 150.0
    assert uniform.freeze_trace_v1() == (
        {
            "ordinal": 0,
            "lower": 100.0,
            "upper": 200.0,
            "u53_integer": 2**52,
            "result": 150.0,
        },
    )


def test_deterministic_uniform_fails_on_missing_extra_or_invalid_bounds() -> None:
    empty = VnpyFacadeDeterministicInputsV1.create(ordered_uniform_draws=())
    with pytest.raises(ValueError, match="DETERMINISTIC_INPUT_INVALID"):
        VnpyFacadeDeterministicUniformV1(empty)(0.0, 1.0)

    one = VnpyFacadeDeterministicInputsV1.create(
        ordered_uniform_draws=(VnpyFacadeUniformDrawV1.create(ordinal=0, u53_integer=0),)
    )
    uniform = VnpyFacadeDeterministicUniformV1(one)
    with pytest.raises(ValueError, match="unconsumed"):
        uniform.freeze_trace_v1()
    with pytest.raises(TypeError, match="not bool"):
        uniform(True, 1.0)
    with pytest.raises(ValueError, match="must be finite"):
        uniform(float("nan"), 1.0)


def test_best_limit_source_requires_explicit_uniform_draw_at_execution_time() -> None:
    cls = load_pinned_vnpy_algorithm_classes_v1()["BEST_LIMIT_MINIQMT"]
    algo = cls.__new__(cls)
    algo.min_volume = 100.0
    algo.max_volume = 200.0

    with pytest.raises(ValueError, match="DETERMINISTIC_INPUT_INVALID"):
        algo.generate_rand_volume()


def test_live_implementation_binding_set_is_exact_and_readback_stable() -> None:
    bindings = build_vnpy_facade_implementation_bindings_v1()

    assert len(bindings) == 30
    assert bindings[0].component_name == "adapter.extract_state"
    assert bindings[-1].component_name == "state_mapping.readback"
    assert (
        readback_vnpy_facade_implementation_bindings_v1([item.model_dump(mode="python") for item in bindings])
        == bindings
    )


def test_hash_correct_live_binding_drift_is_rejected() -> None:
    bindings = build_vnpy_facade_implementation_bindings_v1()
    first = bindings[0]
    forged = VnpyFacadeImplementationBindingV1.create(
        **{
            **first.canonical_payload_v1(exclude={"schema_version", "binding_sha256"}),
            "callable_ref": "forged.module:callable",
        }
    )

    with pytest.raises(ValueError, match="BINDING_INVALID"):
        readback_vnpy_facade_implementation_bindings_v1(
            [forged.model_dump(mode="python"), *[item.model_dump(mode="python") for item in bindings[1:]]]
        )


def test_shared_facade_contract_closes_all_current_k1_surfaces() -> None:
    requirements = tuple(manifest.compatibility_requirement for manifest in current_three_manifests_v2())
    contract = build_vnpy_facade_contract_v1(compatibility_requirements=requirements)
    implementation_hashes = {
        item.component_name: item.binding_sha256 for item in contract.ordered_implementation_bindings
    }
    isolated = build_vnpy_facade_isolated_module_bindings_v1(
        implementation_binding_sha256_by_component=implementation_hashes
    )

    assert len(contract.ordered_method_contracts) == 18
    assert len(isolated) == 14
    assert all(item.export_name not in {"MainEngine", "EventEngine", "OmsEngine", "Gateway"} for item in isolated)
    assert (
        readback_vnpy_facade_contract_v1(
            contract.model_dump(mode="python"),
            compatibility_requirements=requirements,
        )
        == contract
    )


def _characterization_authority():
    requirements = tuple(manifest.compatibility_requirement for manifest in current_three_manifests_v2())
    facade_contract = build_vnpy_facade_contract_v1(compatibility_requirements=requirements)
    source_manifest = build_vnpy_facade_source_manifest_v1()
    state = build_vnpy_facade_state_mappings_v1()
    config_by_algo = {
        "BEST_LIMIT_MINIQMT": {"min_volume": 100, "max_volume": 200},
        "SNIPER_MINIQMT": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
        "TWAP_LITE_MINIQMT": {"time": 600, "interval": 60},
        "ICEBERG": {"display_volume": 100, "interval": 1},
        "STOP": {"price_add": "0.01"},
    }
    schemas = {item.algo_code: thaw_json_v1(item.config_schema) for item in current_three_manifests_v2()}
    schemas.update(
        {
            "ICEBERG": {
                "type": "object",
                "additionalProperties": False,
                "required": ["display_volume", "interval"],
                "properties": {
                    "display_volume": {"type": "integer", "minimum": 0},
                    "interval": {"type": "integer", "minimum": 0},
                },
            },
            "STOP": {
                "type": "object",
                "additionalProperties": False,
                "required": ["price_add"],
                "properties": {"price_add": {"type": "string"}},
            },
        }
    )
    receipts = {}
    vectors = {}
    results = {}
    for source in source_manifest.ordered_sources:
        if source.source_role.value != "ALGORITHM":
            continue
        algo_code = source.algo_code_or_helper_name
        algo_state = tuple(item for item in state if item.algo_code == algo_code)
        schema = schemas[algo_code]
        requirement = VnpyFacadeCharacterizationRequirementV1.create(
            algo_code=algo_code,
            registration_disposition=source.registration_disposition,
            source_identity_sha256=source.source_identity_sha256,
            config_schema_version=f"{algo_code.lower()}_characterization_v1",
            config_schema=schema,
            config_schema_sha256=hash_hex_v1("miniqmt_plugin_config_schema_v1", schema),
            config_validation_contract_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_config_validation_contract_v1", {"algo_code": algo_code}
            ),
            ordered_required_methods=("put_algo_event",),
            ordered_required_object_fields=("TickData.vt_symbol",),
            ordered_required_enum_members=("Direction.LONG",),
            ordered_event_types=("ALGO_START",),
            ordered_market_data_capabilities=("B0_QUOTE_V2",),
            state_mapping_set_sha256=state_mapping_set_sha256_v1(algo_state),
        )
        vector_id = f"{algo_code.lower()}_initialize"
        after_hash = hash_hex_v1("test_k4_after_state", {"algo_code": algo_code})
        vector = VnpyFacadeCharacterizationVectorV1.create(
            vector_id=vector_id,
            algo_code=algo_code,
            side="BUY",
            invocation_phase="INITIALIZE",
            canonical_config=config_by_algo[algo_code],
            before_state_sha256_or_INIT="INIT",
            event_type="ALGO_START",
            event_payload_sha256=hash_hex_v1("test_k4_event", {"algo_code": algo_code}),
            projection_set_sha256="a" * 64,
            authority_input_sha256="b" * 64,
            source_market_data_event_id=None,
            explicit_deterministic_inputs=VnpyFacadeDeterministicInputsV1.create(ordered_uniform_draws=()),
            expected_ordered_facade_calls=({"method": "put_algo_event", "ordinal": 0},),
            expected_ordered_effects=({"effect": "DiagnosticObservationV1", "ordinal": 0},),
            expected_after_state_sha256=after_hash,
            expected_terminal_outcome=None,
        )
        actual = {
            "ordered_facade_calls": [{"method": "put_algo_event", "ordinal": 0}],
            "ordered_effects": [{"effect": "DiagnosticObservationV1", "ordinal": 0}],
            "after_state_sha256": after_hash,
            "terminal_outcome": None,
        }
        receipt = build_vnpy_facade_characterization_receipt_v1(
            requirement=requirement,
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            factory_probe_config=config_by_algo[algo_code],
            vectors=(vector,),
            executed_vector_results={vector_id: actual},
        )
        receipts[algo_code] = receipt
        vectors[algo_code] = (requirement, vector)
        results[algo_code] = actual
    return facade_contract, source_manifest, receipts, vectors, results


def test_caller_supplied_characterization_observation_cannot_self_certify_passed() -> None:
    facade_contract, source_manifest, receipts, vectors, results = _characterization_authority()
    assert all(item.status is VnpyFacadeCompatibilityStatusV1.FAILED for item in receipts.values())
    assert all(
        any(
            failure.reason_code == "MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE"
            for failure in item.ordered_failures
        )
        for item in receipts.values()
    )

    with pytest.raises(VnpyFacadeContractError, match="source execution authority"):
        build_vnpy_facade_algorithm_bindings_v1(
            characterization_receipts=receipts,
            adapter_contract_sha256=facade_contract.facade_contract_sha256,
        )

    requirement, vector = vectors["STOP"]
    assert (
        readback_vnpy_facade_characterization_receipt_v1(
            receipts["STOP"].model_dump(mode="python"),
            requirement=requirement,
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            factory_probe_config={"price_add": "0.01"},
            vectors=(vector,),
            executed_vector_results={vector.vector_id: results["STOP"]},
        )
        == receipts["STOP"]
    )


def test_characterization_observation_rejects_malformed_public_inputs_without_secondary_error() -> None:
    facade_contract, source_manifest, _receipts, vectors, results = _characterization_authority()
    requirement, vector = vectors["STOP"]
    authority = {
        "requirement": requirement,
        "source_manifest": source_manifest,
        "facade_contract": facade_contract,
        "factory_probe_config": {"price_add": "0.01"},
        "vectors": (vector,),
        "executed_vector_results": {vector.vector_id: results["STOP"]},
    }

    with pytest.raises(TypeError, match="source_manifest"):
        build_vnpy_facade_characterization_receipt_v1(**{**authority, "source_manifest": {}})
    with pytest.raises(TypeError, match="vectors"):
        build_vnpy_facade_characterization_receipt_v1(**{**authority, "vectors": [vector]})
    with pytest.raises(TypeError, match="executed_vector_results"):
        build_vnpy_facade_characterization_receipt_v1(
            **{**authority, "executed_vector_results": {vector.vector_id: []}}
        )


def test_characterization_drift_produces_explicit_failed_receipt() -> None:
    facade_contract, source_manifest, receipts, vectors, results = _characterization_authority()
    requirement, vector = vectors["ICEBERG"]
    drifted = {**results["ICEBERG"], "after_state_sha256": "0" * 64}
    failed = build_vnpy_facade_characterization_receipt_v1(
        requirement=requirement,
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        factory_probe_config={"display_volume": 100, "interval": 1},
        vectors=(vector,),
        executed_vector_results={vector.vector_id: drifted},
    )
    assert failed.status is VnpyFacadeCompatibilityStatusV1.FAILED
    assert {item.reason_code for item in failed.ordered_failures} == {
        "MINIQMT_VNPY_FACADE_CHARACTERIZATION_DRIFT",
        "MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE",
    }
    assert receipts["ICEBERG"].status is VnpyFacadeCompatibilityStatusV1.FAILED


def test_k4a_binding_and_conformance_publication_fail_closed_without_source_executor() -> None:
    facade_contract, source_manifest, receipts, _vectors, _results = _characterization_authority()
    runtime = build_plugin_catalog_v2(
        descriptors=current_three_descriptors_v2(),
        creation_bindings=current_three_creation_bindings_v1(),
        process_bindings=current_three_process_bindings_v2(),
        pinned_compatibility_receipts=build_current_three_compatibility_receipts_v1(),
    )
    with pytest.raises(VnpyFacadeContractError, match="source execution authority"):
        build_vnpy_facade_algorithm_bindings_v1(
            characterization_receipts=receipts,
            adapter_contract_sha256=facade_contract.facade_contract_sha256,
        )
    with pytest.raises(VnpyFacadeContractError, match="source execution authority"):
        build_vnpy_facade_conformance_set_v1(
            catalog_runtime=runtime,
            facade_contract=facade_contract,
            source_manifest=source_manifest,
            characterization_receipts=receipts,
            algorithm_bindings={},
        )

    with pytest.raises(TypeError, match="string-keyed mapping"):
        build_vnpy_facade_algorithm_bindings_v1(
            characterization_receipts={1: receipts["STOP"]},  # type: ignore[dict-item]
            adapter_contract_sha256=facade_contract.facade_contract_sha256,
        )
    with pytest.raises(TypeError, match="adapter_contract_sha256"):
        build_vnpy_facade_algorithm_bindings_v1(
            characterization_receipts=receipts,
            adapter_contract_sha256=1,  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError, match="source_root"):
        build_vnpy_facade_algorithm_bindings_v1(
            characterization_receipts=receipts,
            adapter_contract_sha256=facade_contract.facade_contract_sha256,
            source_root="not-a-path",  # type: ignore[arg-type]
        )
