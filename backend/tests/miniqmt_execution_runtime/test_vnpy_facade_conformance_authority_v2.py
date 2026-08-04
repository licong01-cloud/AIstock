from __future__ import annotations

import json
import multiprocessing
from functools import lru_cache
from pathlib import Path

import pytest

import backend.execution_algos.vnpy_compat.facade_characterization as characterization_module
import backend.services.miniqmt_execution_runtime.vnpy_facade_characterization_runner as runner_module
from backend.execution_algos.vnpy_compat.facade_characterization import (
    VnpyFacadeCharacterizationArtifactAuthorityV2,
    build_vnpy_facade_algorithm_bindings_v2,
    build_vnpy_facade_characterization_requirements_v1,
    build_vnpy_facade_conformance_set_v2,
    build_vnpy_facade_contract_v1,
    build_vnpy_facade_source_manifest_v1,
    readback_vnpy_facade_characterization_vector_artifact_v2,
    validate_vnpy_facade_characterization_authority_v2,
    validate_vnpy_facade_conformance_set_against_authority_v2,
)
from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeAlgorithmBindingV2,
    VnpyFacadeAlgorithmCharacterizationReceiptV2,
    VnpyFacadeCharacterizationVectorArtifactV2,
    VnpyFacadeCharacterizationVectorV2,
    VnpyFacadeCompatibilityStatusV1,
    VnpyFacadeConformanceAuthorityV2,
    VnpyFacadeConformanceAuthorityValidationReceiptV2,
    VnpyFacadeConformanceFailureV1,
    VnpyFacadeConformanceReceiptV2,
    VnpyFacadeConformanceSetV2,
    VnpyFacadeContractError,
    VnpyFacadeExecutedVectorResultV1,
    VnpyFacadeSourceExecutionSetV1,
    VnpyFacadeSourceExecutorBindingV1,
)
from backend.execution_algos.vnpy_style.plugin_manifests import current_three_manifests_v3
from backend.execution_algos.vnpy_compat.locked_surface import PINNED_SOURCE_ROOT
from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1
from backend.services.miniqmt_execution_runtime.vnpy_facade_characterization_runner import (
    _MAX_WORKER_CARRIER_BYTES,
    _decode_artifact_json_v1,
    _safe_failure_v1,
    _send_worker_carrier_v1,
    _worker_v1,
    build_vnpy_facade_characterization_authority_fresh_process_v2,
    run_vnpy_facade_source_execution_sets_v1,
    validate_vnpy_facade_conformance_set_fresh_process_v2,
    validate_vnpy_facade_characterization_authority_fresh_process_v2,
    validate_vnpy_facade_k3_expected_trace_materials_v1,
)
from backend.services.miniqmt_execution_runtime.vnpy_facade_diagnostics import (
    _reset_vnpy_facade_diagnostics_for_tests_v1,
    publish_vnpy_facade_characterization_v1,
    publish_vnpy_facade_conformance_v1,
    read_vnpy_facade_diagnostics_v1,
    record_vnpy_facade_conformance_v1,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_creation import _catalog, _gateway


@lru_cache(maxsize=1)
def _authority_inputs():
    source_manifest = build_vnpy_facade_source_manifest_v1()
    facade_contract = build_vnpy_facade_contract_v1(
        compatibility_requirements=tuple(item.compatibility_requirement for item in current_three_manifests_v3())
    )
    requirements = build_vnpy_facade_characterization_requirements_v1(
        catalog_runtime=_catalog(), source_manifest=source_manifest
    )
    return source_manifest, facade_contract, requirements


@pytest.fixture(scope="module")
def characterization_authority():
    source_manifest, facade_contract, requirements = _authority_inputs()
    return build_vnpy_facade_characterization_authority_fresh_process_v2(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        requirements=requirements,
    )


@pytest.fixture(scope="module")
def formal_v2_bundle(characterization_authority):
    source_manifest, facade_contract, _requirements = _authority_inputs()
    bindings = build_vnpy_facade_algorithm_bindings_v2(
        characterization_authority_v2=characterization_authority,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
    )
    conformance_set = build_vnpy_facade_conformance_set_v2(
        catalog_runtime=_catalog(),
        gateway_catalog=_gateway(),
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization_authority,
        algorithm_bindings_v2=bindings,
    )
    sealed = validate_vnpy_facade_conformance_set_against_authority_v2(
        conformance_set=conformance_set,
        catalog_runtime=_catalog(),
        gateway_catalog=_gateway(),
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization_authority,
    )
    return characterization_authority, bindings, conformance_set, sealed


def _strict_reject(model_type, carrier, /, **updates) -> None:
    payload = carrier.model_dump(mode="python")
    payload.update(updates)
    with pytest.raises((TypeError, ValueError)):
        model_type.model_validate(payload, strict=True)


def test_repository_artifact_is_separate_from_k1_source_and_rebuilds_all_k3_material() -> None:
    authority = readback_vnpy_facade_characterization_vector_artifact_v2()

    assert len(authority.artifact.ordered_vectors) == 81
    assert len(authority.artifact.ordered_k3_expected_trace_materials) == 6
    assert characterization_module._VECTOR_ARTIFACT_PATH.parent.name == "characterization_artifacts"
    assert not characterization_module._VECTOR_ARTIFACT_PATH.is_relative_to(PINNED_SOURCE_ROOT)
    validate_vnpy_facade_k3_expected_trace_materials_v1(authority)


def test_fresh_process_authority_executes_exact_five_algorithms_without_partial_publication(
    characterization_authority,
) -> None:
    assert tuple(item.algo_code for item in characterization_authority.receipts) == (
        "BEST_LIMIT_MINIQMT",
        "ICEBERG",
        "SNIPER_MINIQMT",
        "STOP",
        "TWAP_LITE_MINIQMT",
    )
    assert all(item.status is VnpyFacadeCompatibilityStatusV1.PASSED for item in characterization_authority.receipts)
    assert all(
        item.status is VnpyFacadeCompatibilityStatusV1.PASSED
        for item in characterization_authority.source_execution_sets
    )
    assert sum(len(item.ordered_results) for item in characterization_authority.source_execution_sets) == 81


def test_current_three_conformance_rebuild_uses_public_sealed_authority_and_is_restart_stable(
    characterization_authority,
) -> None:
    source_manifest, facade_contract, requirements = _authority_inputs()
    bindings = build_vnpy_facade_algorithm_bindings_v2(
        characterization_authority_v2=characterization_authority,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
    )
    conformance_set = build_vnpy_facade_conformance_set_v2(
        catalog_runtime=_catalog(),
        gateway_catalog=_gateway(),
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization_authority,
        algorithm_bindings_v2=bindings,
    )
    in_process = validate_vnpy_facade_conformance_set_against_authority_v2(
        conformance_set=conformance_set,
        catalog_runtime=_catalog(),
        gateway_catalog=_gateway(),
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization_authority,
    )
    restarted = validate_vnpy_facade_conformance_set_fresh_process_v2(
        conformance_set=conformance_set,
        catalog_runtime=_catalog(),
        gateway_catalog=_gateway(),
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        requirements=requirements,
    )

    assert len(conformance_set.ordered_receipts) == 3
    assert in_process.conformance_set == conformance_set
    assert restarted.conformance_set == conformance_set
    assert restarted.source_executor_binding == characterization_authority.source_executor_binding
    diagnostics = read_vnpy_facade_diagnostics_v1()
    assert diagnostics.active_failure is None
    assert diagnostics.source_manifest_sha256 == source_manifest.manifest_sha256
    assert diagnostics.source_executor_binding_sha256 == (
        characterization_authority.source_executor_binding.binding_sha256
    )
    assert diagnostics.conformance_set_sha256 == conformance_set.receipt_set_sha256
    assert thaw_json_v1(diagnostics.algorithm_statuses) == {
        item.algo_code: "PASSED" for item in characterization_authority.receipts
    }


def test_characterization_receipt_fresh_readback_reexecutes_all_five_algorithms(
    characterization_authority,
) -> None:
    source_manifest, facade_contract, requirements = _authority_inputs()
    restarted = validate_vnpy_facade_characterization_authority_fresh_process_v2(
        receipts=characterization_authority.receipts,
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        requirements=requirements,
    )
    assert restarted.receipts == characterization_authority.receipts
    assert restarted.authority_sha256 == characterization_authority.authority_sha256


def test_hash_correct_characterization_receipt_drift_is_rejected_by_live_authority(
    characterization_authority,
) -> None:
    source_manifest, facade_contract, requirements = _authority_inputs()
    artifact = readback_vnpy_facade_characterization_vector_artifact_v2()
    original = characterization_authority.receipts[0]
    values = original.model_dump(mode="python", exclude={"schema_version", "receipt_sha256"})
    values["source_execution_set_sha256"] = "0" * 64
    forged = VnpyFacadeAlgorithmCharacterizationReceiptV2.create(**values)
    supplied = (forged, *characterization_authority.receipts[1:])

    with pytest.raises(VnpyFacadeContractError, match="fresh five-algorithm execution"):
        validate_vnpy_facade_characterization_authority_v2(
            receipts=supplied,
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            requirements=requirements,
            ordered_vectors=artifact.artifact.ordered_vectors,
            source_executor_binding=characterization_authority.source_executor_binding,
            source_execution_sets=characterization_authority.source_execution_sets,
        )


def test_artifact_strict_readback_rejects_semantic_hash_drift(tmp_path: Path) -> None:
    authority = readback_vnpy_facade_characterization_vector_artifact_v2()
    payload = authority.artifact.model_dump(mode="json")
    payload["k3_source_commit_sha"] = "0" * 40
    artifact_path = tmp_path / "facade_characterization_vectors_v2.json"
    artifact_path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")), encoding="utf-8")

    with pytest.raises(VnpyFacadeContractError, match="failed strict readback"):
        readback_vnpy_facade_characterization_vector_artifact_v2(artifact_path=artifact_path)


def test_artifact_reader_and_authority_carrier_fail_loud_on_malformed_inputs(
    tmp_path: Path,
) -> None:
    authority = readback_vnpy_facade_characterization_vector_artifact_v2()
    with pytest.raises(TypeError, match="pathlib.Path"):
        readback_vnpy_facade_characterization_vector_artifact_v2(artifact_path="bad")  # type: ignore[arg-type]
    with pytest.raises(VnpyFacadeContractError, match="missing"):
        readback_vnpy_facade_characterization_vector_artifact_v2(artifact_path=tmp_path / "missing.json")
    with pytest.raises(TypeError, match="artifact"):
        VnpyFacadeCharacterizationArtifactAuthorityV2(
            artifact=object(),  # type: ignore[arg-type]
            canonical_lf_file_sha256="0" * 64,
        )
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        VnpyFacadeCharacterizationArtifactAuthorityV2(
            artifact=authority.artifact,
            canonical_lf_file_sha256="A" * 64,
        )
    for text in ('{"x":1,"x":2}', "[]", '{"x":NaN}'):
        with pytest.raises((TypeError, ValueError)):
            characterization_module._strict_json_object_v1(text)


def test_source_state_envelope_readback_rejects_owner_order_overlap_and_hash_drift() -> None:
    artifact = readback_vnpy_facade_characterization_vector_artifact_v2().artifact
    state = next(
        item.expected_after_state
        for item in artifact.ordered_vectors
        if len(item.expected_after_state.ordered_parameters) > 1
    )
    _strict_reject(type(state), state, algo_name="wrong_owner")
    _strict_reject(
        type(state),
        state,
        ordered_parameters=tuple(reversed(state.ordered_parameters)),
    )
    _strict_reject(
        type(state),
        state,
        ordered_variables=(state.ordered_parameters[0], *state.ordered_variables),
    )
    _strict_reject(type(state), state, contract_projection_sha256="0" * 64)
    _strict_reject(type(state), state, state_envelope_sha256="0" * 64)


def test_k3_material_hash_correct_carrier_drift_does_not_self_certify() -> None:
    authority = readback_vnpy_facade_characterization_vector_artifact_v2()
    first = authority.artifact.ordered_k3_expected_trace_materials[0]
    receipt = thaw_json_v1(first.parity_receipt)
    receipt["receipt_sha256"] = "0" * 64
    forged_material = first.model_copy(update={"parity_receipt": receipt})
    forged_artifact = authority.artifact.model_copy(
        update={
            "ordered_k3_expected_trace_materials": (
                forged_material,
                *authority.artifact.ordered_k3_expected_trace_materials[1:],
            )
        }
    )
    forged_authority = VnpyFacadeCharacterizationArtifactAuthorityV2(
        artifact=forged_artifact,
        canonical_lf_file_sha256=authority.canonical_lf_file_sha256,
    )

    with pytest.raises(VnpyFacadeContractError, match="failed strict reconstruction"):
        validate_vnpy_facade_k3_expected_trace_materials_v1(forged_authority)


def test_k3_material_must_be_the_exact_expected_trace_ref_for_every_linked_vector() -> None:
    authority = readback_vnpy_facade_characterization_vector_artifact_v2()
    target = next(
        item
        for item in authority.artifact.ordered_vectors
        if item.algo_code == "SNIPER_MINIQMT" and item.side.value == "BUY"
    )
    forged_ref = target.expected_trace_authority_ref.model_copy(update={"authority_identity_sha256": "0" * 64})
    forged_vector = target.model_copy(update={"expected_trace_authority_ref": forged_ref})
    forged_artifact = authority.artifact.model_copy(
        update={
            "ordered_vectors": tuple(
                forged_vector if item.vector_id == target.vector_id else item
                for item in authority.artifact.ordered_vectors
            )
        }
    )
    forged_authority = VnpyFacadeCharacterizationArtifactAuthorityV2(
        artifact=forged_artifact,
        canonical_lf_file_sha256=authority.canonical_lf_file_sha256,
    )

    with pytest.raises(VnpyFacadeContractError, match="failed strict reconstruction"):
        validate_vnpy_facade_k3_expected_trace_materials_v1(forged_authority)


def test_worker_malformed_input_returns_typed_failure_carrier() -> None:
    receive_connection, send_connection = multiprocessing.Pipe(duplex=False)
    try:
        _worker_v1("{}", send_connection)
        carrier = json.loads(receive_connection.recv_bytes().decode("utf-8"))
    finally:
        receive_connection.close()

    assert carrier["status"] == "FAILED"
    assert carrier["failure"]["reason_code"] == "MINIQMT_VNPY_FACADE_SOURCE_EXECUTION_FAILED"
    assert carrier["failure"]["algo_code"] == "<unparsed>"


def test_fresh_runner_timeout_is_explicit_and_never_publishes_partial_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import backend.services.miniqmt_execution_runtime.vnpy_facade_characterization_runner as runner

    source_manifest, facade_contract, requirements = _authority_inputs()
    artifact = readback_vnpy_facade_characterization_vector_artifact_v2()
    monkeypatch.setattr(runner, "_WORKER_TIMEOUT_SECONDS", 0.001)

    with pytest.raises(VnpyFacadeContractError) as caught:
        run_vnpy_facade_source_execution_sets_v1(
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            requirements=requirements,
            artifact_authority=artifact,
        )
    context = caught.value.context
    assert caught.value.reason_code == "MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE"
    assert context["completed_algorithms"] == []
    assert {item["outcome"] for item in context["ordered_failures"]} == {"TIMEOUT"}


def test_runner_input_carriers_and_error_rendering_are_bounded_and_fail_loud() -> None:
    source_manifest, facade_contract, requirements = _authority_inputs()
    artifact = readback_vnpy_facade_characterization_vector_artifact_v2()
    for values, message in (
        (
            {
                "source_manifest": object(),
                "facade_contract": facade_contract,
                "requirements": requirements,
                "artifact_authority": artifact,
            },
            "source_manifest",
        ),
        (
            {
                "source_manifest": source_manifest,
                "facade_contract": object(),
                "requirements": requirements,
                "artifact_authority": artifact,
            },
            "facade_contract",
        ),
        (
            {
                "source_manifest": source_manifest,
                "facade_contract": facade_contract,
                "requirements": list(requirements),
                "artifact_authority": artifact,
            },
            "requirements",
        ),
        (
            {
                "source_manifest": source_manifest,
                "facade_contract": facade_contract,
                "requirements": requirements,
                "artifact_authority": object(),
            },
            "artifact_authority",
        ),
    ):
        with pytest.raises(TypeError, match=message):
            run_vnpy_facade_source_execution_sets_v1(**values)  # type: ignore[arg-type]
    with pytest.raises(VnpyFacadeContractError, match="exactly one requirement"):
        run_vnpy_facade_source_execution_sets_v1(
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            requirements=(requirements[0],),
            artifact_authority=artifact,
        )

    class _BrokenMessage(RuntimeError):
        def __str__(self) -> str:
            raise RuntimeError("secondary renderer failed")

    evidence = _safe_failure_v1(_BrokenMessage(), algo_code="ICEBERG")
    assert evidence["exception_message"] == "<_BrokenMessage: unrenderable>"
    assert evidence["render_error_type"].endswith("RuntimeError")
    assert evidence["message_truncated"] is False
    assert evidence["omitted_message_sha256"] is None


def test_runner_failure_evidence_survives_broken_reason_code_and_closes_truncation() -> None:
    class _BrokenReason(RuntimeError):
        @property
        def reason_code(self) -> str:
            raise LookupError("secondary reason renderer failed")

    root = str(Path(__file__).resolve().parents[3])
    evidence = _safe_failure_v1(_BrokenReason("x" * 2030 + root + "z" * 3000), algo_code="STOP")

    assert evidence["reason_code"] == "MINIQMT_VNPY_FACADE_SOURCE_EXECUTION_FAILED"
    assert root not in evidence["exception_message"]
    assert evidence["message_truncated"] is True
    assert len(evidence["omitted_message_sha256"]) == 64


def test_k3_preflight_failure_records_active_characterization_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    source_manifest, facade_contract, requirements = _authority_inputs()
    artifact = readback_vnpy_facade_characterization_vector_artifact_v2()
    _reset_vnpy_facade_diagnostics_for_tests_v1()
    monkeypatch.setattr(runner_module, "_live_k3_contract_binding_sha256_v1", lambda: "0" * 64)

    with pytest.raises(VnpyFacadeContractError, match="K3 contract binding"):
        run_vnpy_facade_source_execution_sets_v1(
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            requirements=requirements,
            artifact_authority=artifact,
        )

    snapshot = read_vnpy_facade_diagnostics_v1()
    assert snapshot.active_failure is not None
    assert snapshot.active_failure.stage == "CHARACTERIZATION"
    assert snapshot.active_failure.reason_code == "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED"


def test_artifact_readback_failure_records_active_characterization_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    source_manifest, facade_contract, requirements = _authority_inputs()
    _reset_vnpy_facade_diagnostics_for_tests_v1()

    def fail_readback():
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            "artifact readback failed",
            context={"stage": "ARTIFACT_READBACK"},
        )

    monkeypatch.setattr(runner_module, "readback_vnpy_facade_characterization_vector_artifact_v2", fail_readback)
    with pytest.raises(VnpyFacadeContractError, match="artifact readback failed"):
        build_vnpy_facade_characterization_authority_fresh_process_v2(
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            requirements=requirements,
        )

    snapshot = read_vnpy_facade_diagnostics_v1()
    assert snapshot.active_failure is not None
    assert snapshot.active_failure.stage == "CHARACTERIZATION"
    assert snapshot.active_failure.reason_code == "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED"


def test_characterization_publication_only_clears_failure_after_full_conformance(
    characterization_authority,
    formal_v2_bundle,
) -> None:
    _reset_vnpy_facade_diagnostics_for_tests_v1()
    record_vnpy_facade_conformance_v1(
        status="FAILED",
        reason_code="MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID",
    )
    publish_vnpy_facade_characterization_v1(
        authority=characterization_authority,
        source_manifest_sha256=_authority_inputs()[0].manifest_sha256,
        vector_artifact_sha256=readback_vnpy_facade_characterization_vector_artifact_v2().artifact.artifact_sha256,
        vector_artifact_file_sha256=readback_vnpy_facade_characterization_vector_artifact_v2().canonical_lf_file_sha256,
    )
    after_characterization = read_vnpy_facade_diagnostics_v1()
    assert after_characterization.active_failure is not None

    publish_vnpy_facade_conformance_v1(formal_v2_bundle[3])
    after_conformance = read_vnpy_facade_diagnostics_v1()
    assert after_conformance.active_failure is None
    assert after_conformance.last_failure == after_characterization.last_failure


def test_runner_numeric_decoder_and_oversized_carrier_are_explicit() -> None:
    assert _decode_artifact_json_v1({"__k4_numeric_type__": "float", "decimal": "1.25"}) == 1.25
    assert _decode_artifact_json_v1([{"plain": True}, None, 1, "x"]) == [
        {"plain": True},
        None,
        1,
        "x",
    ]
    for value in (
        {"__k4_numeric_type__": "decimal", "decimal": "1.25"},
        {"__k4_numeric_type__": "float", "decimal": "NaN"},
    ):
        with pytest.raises(ValueError, match="numeric marker"):
            _decode_artifact_json_v1(value)
    with pytest.raises(TypeError, match="unsupported"):
        _decode_artifact_json_v1(1.25)

    class _CaptureConnection:
        def __init__(self) -> None:
            self.payload = b""

        def send_bytes(self, payload: bytes) -> None:
            self.payload = payload

    capture = _CaptureConnection()
    _send_worker_carrier_v1(
        capture,
        {
            "status": "OK",
            "algo_code": "ICEBERG",
            "execution_set": "x" * (_MAX_WORKER_CARRIER_BYTES + 1),
        },
    )
    carrier = json.loads(capture.payload.decode("utf-8"))
    assert carrier["status"] == "FAILED"
    assert carrier["failure"]["outcome"] == "CARRIER_SIZE_EXCEEDED"
    assert carrier["failure"]["actual_carrier_bytes"] > _MAX_WORKER_CARRIER_BYTES


def test_v2_execution_receipt_and_binding_readbacks_reject_all_semantic_drift(
    formal_v2_bundle,
) -> None:
    authority, bindings, _conformance_set, _sealed = formal_v2_bundle
    failure = VnpyFacadeConformanceFailureV1.create(
        field_path="execution",
        reason_code="MINIQMT_VNPY_FACADE_SOURCE_EXECUTION_FAILED",
        context={"outcome": "TEST_ONLY_NEGATIVE"},
    )
    execution_set = authority.source_execution_sets[0]
    result = execution_set.ordered_results[0]
    _strict_reject(
        VnpyFacadeExecutedVectorResultV1,
        result,
        ordered_execution_failures=(failure,),
    )
    _strict_reject(VnpyFacadeExecutedVectorResultV1, result, actual_after_state_or_null=None)
    _strict_reject(VnpyFacadeExecutedVectorResultV1, result, result_sha256="0" * 64)
    failed_result = VnpyFacadeExecutedVectorResultV1.create(
        **{
            **result.model_dump(
                mode="python",
                exclude={
                    "schema_version",
                    "invocation_status",
                    "actual_after_state_or_null",
                    "ordered_execution_failures",
                    "result_sha256",
                },
            ),
            "invocation_status": "FAILED",
            "actual_after_state_or_null": None,
            "ordered_execution_failures": (failure,),
        }
    )
    _strict_reject(
        VnpyFacadeExecutedVectorResultV1,
        failed_result,
        ordered_execution_failures=(),
    )
    _strict_reject(VnpyFacadeSourceExecutionSetV1, execution_set, ordered_results=())
    _strict_reject(
        VnpyFacadeSourceExecutionSetV1,
        execution_set,
        ordered_results=(failed_result,),
    )
    _strict_reject(
        VnpyFacadeSourceExecutionSetV1,
        execution_set,
        status=VnpyFacadeCompatibilityStatusV1.FAILED,
    )
    _strict_reject(
        VnpyFacadeSourceExecutionSetV1,
        execution_set,
        ordered_results=(result, result),
    )
    _strict_reject(
        VnpyFacadeSourceExecutionSetV1,
        execution_set,
        execution_set_sha256="0" * 64,
    )

    executor = authority.source_executor_binding
    _strict_reject(
        VnpyFacadeSourceExecutorBindingV1,
        executor,
        supported_algo_codes=executor.supported_algo_codes[:-1],
    )
    _strict_reject(VnpyFacadeSourceExecutorBindingV1, executor, binding_sha256="0" * 64)
    receipt = authority.receipts[0]
    _strict_reject(
        VnpyFacadeAlgorithmCharacterizationReceiptV2,
        receipt,
        ordered_failures=(failure,),
    )
    _strict_reject(
        VnpyFacadeAlgorithmCharacterizationReceiptV2,
        receipt,
        ordered_vector_ids=(),
    )
    _strict_reject(
        VnpyFacadeAlgorithmCharacterizationReceiptV2,
        receipt,
        status=VnpyFacadeCompatibilityStatusV1.FAILED,
    )
    _strict_reject(
        VnpyFacadeAlgorithmCharacterizationReceiptV2,
        receipt,
        factory_probe_config_sha256="0" * 64,
    )
    _strict_reject(
        VnpyFacadeAlgorithmCharacterizationReceiptV2,
        receipt,
        receipt_sha256="0" * 64,
    )
    _strict_reject(VnpyFacadeAlgorithmBindingV2, bindings[0], binding_sha256="0" * 64)


def test_v2_vector_and_artifact_readbacks_reject_identity_matrix_drift() -> None:
    artifact = readback_vnpy_facade_characterization_vector_artifact_v2().artifact
    initial = next(item for item in artifact.ordered_vectors if item.invocation_phase == "INITIALIZE")
    transition = next(
        item
        for item in artifact.ordered_vectors
        if item.invocation_phase == "TRANSITION" and item.expected_ordered_facade_calls
    )
    effect_transition = next(
        item
        for item in artifact.ordered_vectors
        if item.invocation_phase == "TRANSITION" and item.expected_ordered_effects
    )
    non_initial = next(item for item in artifact.ordered_vectors if item.step_ordinal > 0)

    _strict_reject(
        VnpyFacadeCharacterizationVectorV2,
        initial,
        predecessor_vector_id_or_INIT="not_init",
    )
    _strict_reject(
        VnpyFacadeCharacterizationVectorV2,
        non_initial,
        predecessor_vector_id_or_INIT="INIT",
    )
    _strict_reject(
        VnpyFacadeCharacterizationVectorV2,
        initial,
        runtime_event_or_null=transition.runtime_event_or_null,
    )
    _strict_reject(
        VnpyFacadeCharacterizationVectorV2,
        initial,
        start_context_or_null=initial.start_context_or_null.model_copy(update={"vector_id": "wrong_vector"}),
    )
    _strict_reject(
        VnpyFacadeCharacterizationVectorV2,
        transition,
        start_context_or_null=initial.start_context_or_null,
    )
    _strict_reject(VnpyFacadeCharacterizationVectorV2, transition, runtime_event_or_null=None)
    _strict_reject(
        VnpyFacadeCharacterizationVectorV2,
        transition,
        runtime_event_or_null=transition.runtime_event_or_null.model_copy(update={"runtime_id": "runtime_drift"}),
    )
    _strict_reject(
        VnpyFacadeCharacterizationVectorV2,
        transition,
        expected_ordered_facade_calls=(
            transition.expected_ordered_facade_calls[0].model_copy(update={"ordinal": 7}),
            *transition.expected_ordered_facade_calls[1:],
        ),
    )
    _strict_reject(
        VnpyFacadeCharacterizationVectorV2,
        effect_transition,
        expected_ordered_effects=(
            effect_transition.expected_ordered_effects[0].model_copy(update={"ordinal": 7}),
            *effect_transition.expected_ordered_effects[1:],
        ),
    )
    _strict_reject(VnpyFacadeCharacterizationVectorV2, transition, vector_sha256="0" * 64)

    vectors = artifact.ordered_vectors
    materials = artifact.ordered_k3_expected_trace_materials
    _strict_reject(VnpyFacadeCharacterizationVectorArtifactV2, artifact, k3_source_commit_sha="A" * 40)
    _strict_reject(
        VnpyFacadeCharacterizationVectorArtifactV2,
        artifact,
        ordered_k3_expected_trace_materials=tuple(reversed(materials)),
    )
    _strict_reject(
        VnpyFacadeCharacterizationVectorArtifactV2,
        artifact,
        ordered_vectors=tuple(reversed(vectors)),
    )
    _strict_reject(
        VnpyFacadeCharacterizationVectorArtifactV2,
        artifact,
        ordered_vectors=tuple(item for item in vectors if item.algo_code != "STOP"),
    )
    stop_vector = next(item for item in vectors if item.algo_code == "STOP")
    k3_ref = next(item.expected_trace_authority_ref for item in vectors if item.algo_code == "BEST_LIMIT_MINIQMT")
    _strict_reject(
        VnpyFacadeCharacterizationVectorArtifactV2,
        artifact,
        ordered_vectors=tuple(
            stop_vector.model_copy(update={"expected_trace_authority_ref": k3_ref})
            if item.vector_id == stop_vector.vector_id
            else item
            for item in vectors
        ),
    )
    best_vector = next(item for item in vectors if item.algo_code == "BEST_LIMIT_MINIQMT")
    _strict_reject(
        VnpyFacadeCharacterizationVectorArtifactV2,
        artifact,
        ordered_vectors=tuple(
            best_vector.model_copy(
                update={
                    "expected_trace_authority_ref": best_vector.expected_trace_authority_ref.model_copy(
                        update={"authority_identity_sha256": "0" * 64}
                    )
                }
            )
            if item.vector_id == best_vector.vector_id
            else item
            for item in vectors
        ),
    )
    _strict_reject(
        VnpyFacadeCharacterizationVectorArtifactV2,
        artifact,
        vector_set_sha256="0" * 64,
    )
    _strict_reject(
        VnpyFacadeCharacterizationVectorArtifactV2,
        artifact,
        artifact_sha256="0" * 64,
    )


def test_v2_conformance_publication_carriers_reject_partial_or_forged_state(
    formal_v2_bundle,
) -> None:
    characterization_authority, _bindings, conformance_set, authority = formal_v2_bundle
    failure = VnpyFacadeConformanceFailureV1.create(
        field_path="conformance",
        reason_code="MINIQMT_VNPY_FACADE_CONFORMANCE_DRIFT",
        context={"outcome": "TEST_ONLY_NEGATIVE"},
    )
    receipt = conformance_set.ordered_receipts[0]
    _strict_reject(
        VnpyFacadeConformanceReceiptV2,
        receipt,
        command_authority_disposition="NOT_APPLICABLE_PURE_PLUGIN",
    )
    _strict_reject(VnpyFacadeConformanceReceiptV2, receipt, ordered_failures=(failure,))
    _strict_reject(
        VnpyFacadeConformanceReceiptV2,
        receipt,
        status=VnpyFacadeCompatibilityStatusV1.FAILED,
    )
    _strict_reject(VnpyFacadeConformanceReceiptV2, receipt, receipt_sha256="0" * 64)
    _strict_reject(
        VnpyFacadeConformanceSetV2,
        conformance_set,
        ordered_receipts=tuple(reversed(conformance_set.ordered_receipts)),
    )
    _strict_reject(VnpyFacadeConformanceSetV2, conformance_set, ordered_receipts=())
    _strict_reject(
        VnpyFacadeConformanceSetV2,
        conformance_set,
        ordered_source_execution_set_sha256s=(
            *conformance_set.ordered_source_execution_set_sha256s,
            conformance_set.ordered_source_execution_set_sha256s[0],
        ),
    )
    _strict_reject(
        VnpyFacadeConformanceSetV2,
        conformance_set,
        source_executor_binding_sha256="0" * 64,
    )
    _strict_reject(VnpyFacadeConformanceSetV2, conformance_set, receipt_set_sha256="0" * 64)

    validation = authority.validation_receipt
    with pytest.raises(ValueError, match="unique"):
        VnpyFacadeConformanceAuthorityValidationReceiptV2.create(
            **{
                **validation.model_dump(
                    mode="python", exclude={"schema_version", "receipt_sha256", "ordered_source_execution_set_sha256s"}
                ),
                "ordered_source_execution_set_sha256s": (
                    validation.ordered_source_execution_set_sha256s[0],
                    validation.ordered_source_execution_set_sha256s[0],
                ),
            }
        )
    _strict_reject(
        VnpyFacadeConformanceAuthorityValidationReceiptV2,
        validation,
        ordered_source_execution_set_sha256s=(),
    )
    _strict_reject(
        VnpyFacadeConformanceAuthorityValidationReceiptV2,
        validation,
        ordered_failures=(failure,),
    )
    _strict_reject(
        VnpyFacadeConformanceAuthorityValidationReceiptV2,
        validation,
        status=VnpyFacadeCompatibilityStatusV1.FAILED,
    )
    _strict_reject(
        VnpyFacadeConformanceAuthorityValidationReceiptV2,
        validation,
        receipt_sha256="0" * 64,
    )
    with pytest.raises(TypeError, match="only be created"):
        VnpyFacadeConformanceAuthorityV2(
            token=object(),
            conformance_set=conformance_set,
            source_executor_binding=authority.source_executor_binding,
            source_execution_sets=authority.source_execution_sets,
            characterization_receipts=characterization_authority.receipts,
            algorithm_bindings=authority.algorithm_bindings,
            validation_receipt=validation,
        )
    with pytest.raises(AttributeError, match="immutable"):
        authority._conformance_set = conformance_set  # type: ignore[misc]
    with pytest.raises(VnpyFacadeContractError, match="one exact plugin receipt"):
        authority.receipt_for_plugin_v2(
            _catalog().snapshot.registration_descriptors[-1].plugin_key.model_copy(update={"plugin_id": "missing"})
        )
    with pytest.raises(VnpyFacadeContractError, match="one exact algorithm binding"):
        authority.binding_for_algo_v2("MISSING_ALGO")
