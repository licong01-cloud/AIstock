"""Fresh-process orchestration for K4-B pinned-source characterization."""

from __future__ import annotations

import json
import multiprocessing
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from backend.execution_algos.vnpy_compat.facade_characterization import (
    VnpyFacadeCharacterizationAuthorityV2,
    VnpyFacadeCharacterizationArtifactAuthorityV2,
    readback_vnpy_facade_characterization_vector_artifact_v2,
)
from backend.execution_algos.vnpy_compat.facade_contracts import (
    VnpyFacadeCharacterizationRequirementV1,
    VnpyFacadeCharacterizationVectorV2,
    VnpyFacadeContractError,
    VnpyFacadeContractV1,
    VnpyFacadeSourceExecutionSetV1,
    VnpyFacadeSourceManifestV1,
    VnpyFacadeSourceExecutorBindingV1,
    VnpyFacadeAlgorithmCharacterizationReceiptV2,
    VnpyFacadeConformanceAuthorityV2,
    VnpyFacadeConformanceSetV2,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    GatewayCapabilityCatalogV1,
    bounded_exception_summary_v1,
    stable_exception_reason_code_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import PluginCatalogRuntimeV2
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1, thaw_json_v1
from backend.services.miniqmt_execution_runtime.vnpy_facade_diagnostics import (
    publish_vnpy_facade_characterization_v1,
    publish_vnpy_facade_conformance_v1,
    record_vnpy_facade_characterization_build_v1,
    record_vnpy_facade_conformance_v1,
    record_vnpy_facade_source_execution_v1,
)

_K3_SOURCE_COMMIT = "38434e10d530edd883fa75f904de5b025158f918"

_WORKER_TIMEOUT_SECONDS = 120
_WORKER_EXIT_GRACE_SECONDS = 5
_MAX_WORKER_CARRIER_BYTES = 8 * 1024 * 1024
_SUPPORTED_ALGOS = (
    "BEST_LIMIT_MINIQMT",
    "ICEBERG",
    "SNIPER_MINIQMT",
    "STOP",
    "TWAP_LITE_MINIQMT",
)


def _safe_failure_v1(exc: Exception, *, algo_code: str) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[3]
    summary = bounded_exception_summary_v1(
        exc,
        redacted_values=(str(root), str(root).replace("\\", "\\\\"), root.as_posix()),
    )
    render_error_type = summary.pop("renderer_error_type")
    return {
        "algo_code": algo_code,
        "reason_code": stable_exception_reason_code_v1(
            exc,
            default="MINIQMT_VNPY_FACADE_SOURCE_EXECUTION_FAILED",
        ),
        **summary,
        "render_error_type": render_error_type,
    }


def _decode_artifact_json_v1(value: Any) -> Any:
    if value is None or type(value) in (bool, int, str):
        return value
    if isinstance(value, list):
        return [_decode_artifact_json_v1(item) for item in value]
    if isinstance(value, dict):
        if set(value) == {"__k4_numeric_type__", "decimal"}:
            if value["__k4_numeric_type__"] != "float" or type(value["decimal"]) is not str:
                raise ValueError("K3 artifact numeric marker is malformed")
            decimal = Decimal(value["decimal"])
            if not decimal.is_finite():
                raise ValueError("K3 artifact numeric marker is non-finite")
            return float(decimal)
        return {key: _decode_artifact_json_v1(member) for key, member in value.items()}
    raise TypeError(f"unsupported K3 artifact carrier {type(value).__name__}")


def _live_k3_contract_binding_sha256_v1() -> str:
    from backend.execution_algos.vnpy_style.plugin_factories import current_three_process_bindings_v3
    from backend.execution_algos.vnpy_style.plugin_manifests import (
        current_three_creation_bindings_v3,
        current_three_manifests_v3,
    )
    from backend.services.miniqmt_execution_runtime.plugin_registry import (
        callable_ref_v1,
        callable_signature_sha256_v1,
    )

    bindings = current_three_process_bindings_v3()
    process_items = []
    for binding_id in bindings.binding_ids:
        value = bindings.resolve(binding_id)
        if value is None:
            raise ValueError("current-three process binding disappeared during live readback")
        process_items.append(
            {
                "binding_id": binding_id,
                "callable_ref": callable_ref_v1(value),
                "signature_sha256": callable_signature_sha256_v1(value),
            }
        )
    return hash_hex_v1(
        "miniqmt_k3_current_three_contract_binding_v1",
        {
            "manifests": [item.canonical_payload_v1() for item in current_three_manifests_v3()],
            "creation_bindings": [item.canonical_payload_v1() for item in current_three_creation_bindings_v3()],
            "process_bindings": process_items,
        },
    )


def validate_vnpy_facade_k3_expected_trace_materials_v1(
    artifact_authority: VnpyFacadeCharacterizationArtifactAuthorityV2,
) -> None:
    """Rebuild all six BUY/SELL K3 committed-fact receipts from artifact material."""

    from backend.services.miniqmt_execution_runtime.kernel_current_three_contracts import (
        CurrentThreeParityInputV1,
        CurrentThreeParityReceiptV1,
        CurrentThreeShadowSourceSnapshotV1,
    )
    from backend.services.miniqmt_execution_runtime.kernel_current_three_shadow_runner import (
        build_current_three_parity_input_from_shadow_v1,
        run_current_three_committed_parity_v1,
    )
    from backend.services.miniqmt_execution_runtime.kernel_current_three_shadow_source import (
        build_current_three_shadow_repository_read_v1,
    )
    from backend.services.miniqmt_execution_runtime.models import (
        MiniQMTChildOrder,
        MiniQMTExecutionAlgoInstance,
        MiniQMTExecutionEvent,
        MiniQMTExecutionRuntimeRecord,
    )

    artifact = artifact_authority.artifact
    if artifact.k3_source_commit_sha != _K3_SOURCE_COMMIT:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            "vector artifact K3 source commit differs from the frozen K3 authority",
            context={"expected": _K3_SOURCE_COMMIT, "actual": artifact.k3_source_commit_sha},
        )
    live_binding = _live_k3_contract_binding_sha256_v1()
    if artifact.k3_contract_binding_sha256 != live_binding:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            "vector artifact K3 contract binding differs from current production constructors",
            context={
                "expected": live_binding,
                "actual": artifact.k3_contract_binding_sha256,
            },
        )
    failures: list[dict[str, Any]] = []
    for material in artifact.ordered_k3_expected_trace_materials:
        try:

            def decode_model(model_type: Any, carrier: Any) -> Any:
                decoded = _decode_artifact_json_v1(thaw_json_v1(carrier))
                return model_type.model_validate_json(
                    json.dumps(decoded, sort_keys=True, separators=(",", ":")), strict=True
                )

            snapshot = decode_model(CurrentThreeShadowSourceSnapshotV1, material.source_snapshot)
            runtime = decode_model(MiniQMTExecutionRuntimeRecord, material.repository_runtime)
            events = tuple(decode_model(MiniQMTExecutionEvent, item) for item in material.ordered_repository_events)
            algos = tuple(
                decode_model(MiniQMTExecutionAlgoInstance, item) for item in material.ordered_repository_algos
            )
            children = tuple(decode_model(MiniQMTChildOrder, item) for item in material.ordered_repository_children)
            if len(algos) != 1:
                raise ValueError("K3 artifact material requires exactly one algorithm instance")
            read = build_current_three_shadow_repository_read_v1(
                repository_commit_sha=artifact.k3_source_commit_sha,
                runtime=runtime,
                events=events,
                algos=algos,
                children=children,
                database_snapshot_at_utc=datetime.fromisoformat(
                    snapshot.database_snapshot_at_utc.replace("Z", "+00:00")
                ),
            )
            if read.snapshot != snapshot:
                raise ValueError("K3 artifact repository material does not reproduce its source snapshot")
            parity_input, _ = build_current_three_parity_input_from_shadow_v1(
                read, legacy_algo_instance_id=algos[0].algo_instance_id
            )
            supplied_input = decode_model(CurrentThreeParityInputV1, material.parity_input)
            if parity_input != supplied_input:
                raise ValueError("K3 artifact parity input differs from committed-fact reconstruction")
            receipt = run_current_three_committed_parity_v1(read, legacy_algo_instance_id=algos[0].algo_instance_id)
            supplied_receipt = decode_model(CurrentThreeParityReceiptV1, material.parity_receipt)
            if receipt != supplied_receipt:
                raise ValueError("K3 artifact parity receipt differs from current K3 execution")
            expected_ref_values = (
                material.material_sha256,
                snapshot.source_set_sha256,
                supplied_input.input_sha256,
                supplied_receipt.receipt_sha256,
            )
            linked_vectors = tuple(
                item
                for item in artifact.ordered_vectors
                if item.algo_code == material.algo_code and item.side is material.side
            )
            if not linked_vectors:
                raise ValueError("K3 material has no current-three characterization vectors")
            for vector in linked_vectors:
                ref = vector.expected_trace_authority_ref
                actual_ref_values = (
                    ref.authority_identity_sha256,
                    ref.source_snapshot_sha256_or_null,
                    ref.parity_input_sha256_or_null,
                    ref.parity_receipt_sha256_or_null,
                )
                if ref.authority_kind != "K3_COMMITTED_PARITY" or actual_ref_values != expected_ref_values:
                    raise ValueError("current-three vector expected trace ref differs from its exact K3 material")
        except Exception as exc:
            failures.append(_safe_failure_v1(exc, algo_code=f"{material.algo_code}:{material.side.value}"))
    if failures:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            "one or more K3 committed-fact expected-trace materials failed strict reconstruction",
            context={"ordered_failures": failures},
        )


def _send_worker_carrier_v1(output: Any, carrier: dict[str, Any]) -> None:
    encoded = json.dumps(carrier, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(encoded) > _MAX_WORKER_CARRIER_BYTES:
        encoded = json.dumps(
            {
                "status": "FAILED",
                "failure": {
                    "algo_code": carrier.get("algo_code", "<unknown>"),
                    "reason_code": "MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE",
                    "outcome": "CARRIER_SIZE_EXCEEDED",
                    "max_carrier_bytes": _MAX_WORKER_CARRIER_BYTES,
                    "actual_carrier_bytes": len(encoded),
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    output.send_bytes(encoded)


def _worker_v1(payload_json: str, output: Any) -> None:
    algo_code = "<unparsed>"
    try:
        payload = json.loads(payload_json)
        if type(payload) is not dict or set(payload) != {
            "algo_code",
            "source_manifest",
            "facade_contract",
            "requirement",
            "vectors",
            "source_executor_binding",
        }:
            raise ValueError("characterization worker input schema is invalid")
        algo_code = payload["algo_code"]

        def read_json(model_type: Any, value: Any) -> Any:
            return model_type.model_validate_json(json.dumps(value, sort_keys=True, separators=(",", ":")), strict=True)

        source_manifest = read_json(VnpyFacadeSourceManifestV1, payload["source_manifest"])
        facade_contract = read_json(VnpyFacadeContractV1, payload["facade_contract"])
        requirement = read_json(VnpyFacadeCharacterizationRequirementV1, payload["requirement"])
        vectors = tuple(read_json(VnpyFacadeCharacterizationVectorV2, item) for item in payload["vectors"])
        source_executor_binding = read_json(VnpyFacadeSourceExecutorBindingV1, payload["source_executor_binding"])
        if requirement.algo_code != algo_code or any(item.algo_code != algo_code for item in vectors):
            raise ValueError("characterization worker algorithm owner drifted")
        from backend.execution_algos.vnpy_compat.facade_source_execution import (
            execute_vnpy_facade_source_vectors_v1,
        )

        result = execute_vnpy_facade_source_vectors_v1(
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            requirements=(requirement,),
            ordered_vectors=vectors,
            source_executor_binding=source_executor_binding,
        )
        if len(result) != 1 or result[0].algo_code != algo_code:
            raise ValueError("characterization worker returned an invalid result set")
        _send_worker_carrier_v1(
            output,
            {"status": "OK", "algo_code": algo_code, "execution_set": result[0].model_dump(mode="json")},
        )
    except Exception as exc:
        _send_worker_carrier_v1(
            output,
            {"status": "FAILED", "failure": _safe_failure_v1(exc, algo_code=algo_code)},
        )
    finally:
        output.close()


def _run_vnpy_facade_source_execution_sets_unrecorded_v1(
    *,
    source_manifest: VnpyFacadeSourceManifestV1,
    facade_contract: VnpyFacadeContractV1,
    requirements: tuple[VnpyFacadeCharacterizationRequirementV1, ...],
    artifact_authority: VnpyFacadeCharacterizationArtifactAuthorityV2,
) -> tuple[VnpyFacadeSourceExecutionSetV1, ...]:
    """Run one bounded fresh interpreter per algorithm and publish only a full five-set result."""

    if not isinstance(source_manifest, VnpyFacadeSourceManifestV1):
        raise TypeError("source_manifest must be VnpyFacadeSourceManifestV1")
    if not isinstance(facade_contract, VnpyFacadeContractV1):
        raise TypeError("facade_contract must be VnpyFacadeContractV1")
    if type(requirements) is not tuple or any(
        not isinstance(item, VnpyFacadeCharacterizationRequirementV1) for item in requirements
    ):
        raise TypeError("requirements must be a tuple of VnpyFacadeCharacterizationRequirementV1")
    if not isinstance(artifact_authority, VnpyFacadeCharacterizationArtifactAuthorityV2):
        raise TypeError("artifact_authority must be VnpyFacadeCharacterizationArtifactAuthorityV2")
    validate_vnpy_facade_k3_expected_trace_materials_v1(artifact_authority)
    ordered_vectors = artifact_authority.artifact.ordered_vectors
    requirement_by_algo = {item.algo_code: item for item in requirements}
    if tuple(sorted(requirement_by_algo)) != _SUPPORTED_ALGOS or len(requirement_by_algo) != len(requirements):
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_SOURCE_EXECUTOR_INVALID",
            "fresh-process build requires exactly one requirement for each of five algorithms",
            context={"actual_algo_codes": sorted(requirement_by_algo)},
        )
    from backend.execution_algos.vnpy_compat.facade_source_execution import (
        build_vnpy_facade_source_executor_binding_v1,
    )

    source_executor_binding = build_vnpy_facade_source_executor_binding_v1(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        vector_artifact_sha256=artifact_authority.artifact.artifact_sha256,
        vector_artifact_file_sha256=artifact_authority.canonical_lf_file_sha256,
    )
    context = multiprocessing.get_context("spawn")
    execution_sets: list[VnpyFacadeSourceExecutionSetV1] = []
    failures: list[dict[str, Any]] = []
    for algo_code in _SUPPORTED_ALGOS:
        vectors = tuple(item for item in ordered_vectors if item.algo_code == algo_code)
        worker_payload = {
            "algo_code": algo_code,
            "source_manifest": source_manifest.model_dump(mode="json"),
            "facade_contract": facade_contract.model_dump(mode="json"),
            "requirement": requirement_by_algo[algo_code].model_dump(mode="json"),
            "vectors": [item.model_dump(mode="json") for item in vectors],
            "source_executor_binding": source_executor_binding.model_dump(mode="json"),
        }
        receive_connection, send_connection = context.Pipe(duplex=False)
        process = context.Process(
            target=_worker_v1,
            args=(json.dumps(worker_payload, sort_keys=True, separators=(",", ":")), send_connection),
            daemon=False,
        )
        process.start()
        send_connection.close()
        carrier_json: str | None = None
        try:
            if receive_connection.poll(_WORKER_TIMEOUT_SECONDS):
                carrier_json = receive_connection.recv_bytes(maxlength=_MAX_WORKER_CARRIER_BYTES).decode(
                    "utf-8", errors="strict"
                )
        except (EOFError, OSError, UnicodeDecodeError) as exc:
            failures.append(_safe_failure_v1(exc, algo_code=algo_code))
        finally:
            receive_connection.close()
        if carrier_json is None and process.is_alive():
            process.terminate()
            process.join(_WORKER_EXIT_GRACE_SECONDS)
            failures.append(
                {
                    "algo_code": algo_code,
                    "reason_code": "MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE",
                    "outcome": "TIMEOUT",
                    "timeout_seconds": _WORKER_TIMEOUT_SECONDS,
                }
            )
            continue
        process.join(_WORKER_EXIT_GRACE_SECONDS)
        if process.is_alive():
            process.terminate()
            process.join(_WORKER_EXIT_GRACE_SECONDS)
            failures.append(
                {
                    "algo_code": algo_code,
                    "reason_code": "MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE",
                    "outcome": "POST_CARRIER_EXIT_TIMEOUT",
                    "exit_code": process.exitcode,
                }
            )
            continue
        if carrier_json is None:
            failures.append(
                {
                    "algo_code": algo_code,
                    "reason_code": "MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE",
                    "outcome": "NO_CARRIER",
                    "exit_code": process.exitcode,
                }
            )
            continue
        try:
            carrier = json.loads(carrier_json)
            if type(carrier) is not dict or carrier.get("status") not in {"OK", "FAILED"}:
                raise ValueError("worker output envelope is malformed")
            if carrier["status"] == "FAILED":
                failure = carrier.get("failure")
                if type(failure) is not dict:
                    raise ValueError("worker failure carrier is malformed")
                failures.append(failure)
                continue
            if set(carrier) != {"status", "algo_code", "execution_set"} or carrier["algo_code"] != algo_code:
                raise ValueError("worker success carrier owner/schema is malformed")
            execution_set = VnpyFacadeSourceExecutionSetV1.model_validate_json(
                json.dumps(carrier["execution_set"], sort_keys=True, separators=(",", ":")),
                strict=True,
            )
            if execution_set.algo_code != algo_code:
                raise ValueError("worker execution set owner drifted")
            execution_sets.append(execution_set)
            record_vnpy_facade_source_execution_v1(
                algo_code=algo_code,
                status="PASSED",
                reason_code="NONE",
            )
        except Exception as exc:
            failures.append(_safe_failure_v1(exc, algo_code=algo_code))
    if failures or tuple(item.algo_code for item in execution_sets) != _SUPPORTED_ALGOS:
        failed_algorithms = {item.get("algo_code") for item in failures if item.get("algo_code") in _SUPPORTED_ALGOS}
        for algo_code in sorted(failed_algorithms):
            record_vnpy_facade_source_execution_v1(
                algo_code=algo_code,
                status="FAILED",
                reason_code="MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE",
            )
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE",
            "fresh-process characterization did not produce the complete five-algorithm authority",
            context={"ordered_failures": failures, "completed_algorithms": [item.algo_code for item in execution_sets]},
        )
    return tuple(execution_sets)


def run_vnpy_facade_source_execution_sets_v1(
    *,
    source_manifest: VnpyFacadeSourceManifestV1,
    facade_contract: VnpyFacadeContractV1,
    requirements: tuple[VnpyFacadeCharacterizationRequirementV1, ...],
    artifact_authority: VnpyFacadeCharacterizationArtifactAuthorityV2,
) -> tuple[VnpyFacadeSourceExecutionSetV1, ...]:
    """Run the complete five-algorithm authority and retain aggregate failure state."""

    try:
        return _run_vnpy_facade_source_execution_sets_unrecorded_v1(
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            requirements=requirements,
            artifact_authority=artifact_authority,
        )
    except VnpyFacadeContractError as exc:
        record_vnpy_facade_characterization_build_v1(
            status="FAILED",
            reason_code=stable_exception_reason_code_v1(
                exc,
                default="MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            ),
        )
        raise


def _read_artifact_authority_with_diagnostics_v1() -> VnpyFacadeCharacterizationArtifactAuthorityV2:
    try:
        return readback_vnpy_facade_characterization_vector_artifact_v2()
    except VnpyFacadeContractError as exc:
        record_vnpy_facade_characterization_build_v1(
            status="FAILED",
            reason_code=stable_exception_reason_code_v1(
                exc,
                default="MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            ),
        )
        raise


def build_vnpy_facade_characterization_authority_fresh_process_v2(
    *,
    source_manifest: VnpyFacadeSourceManifestV1,
    facade_contract: VnpyFacadeContractV1,
    requirements: tuple[VnpyFacadeCharacterizationRequirementV1, ...],
) -> VnpyFacadeCharacterizationAuthorityV2:
    """Own fresh-process execution and pass its strict result to the pure authority builder."""

    from backend.execution_algos.vnpy_compat.facade_characterization import (
        build_vnpy_facade_characterization_authority_v2,
    )

    artifact_authority = _read_artifact_authority_with_diagnostics_v1()
    execution_sets = run_vnpy_facade_source_execution_sets_v1(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        requirements=requirements,
        artifact_authority=artifact_authority,
    )
    from backend.execution_algos.vnpy_compat.facade_source_execution import (
        build_vnpy_facade_source_executor_binding_v1,
    )

    try:
        binding = build_vnpy_facade_source_executor_binding_v1(
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            vector_artifact_sha256=artifact_authority.artifact.artifact_sha256,
            vector_artifact_file_sha256=artifact_authority.canonical_lf_file_sha256,
        )
        authority = build_vnpy_facade_characterization_authority_v2(
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            requirements=requirements,
            ordered_vectors=artifact_authority.artifact.ordered_vectors,
            source_executor_binding=binding,
            source_execution_sets=execution_sets,
        )
    except VnpyFacadeContractError as exc:
        record_vnpy_facade_characterization_build_v1(
            status="FAILED",
            reason_code=stable_exception_reason_code_v1(
                exc,
                default="MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            ),
        )
        raise
    record_vnpy_facade_characterization_build_v1(status="PASSED", reason_code="NONE")
    publish_vnpy_facade_characterization_v1(
        authority=authority,
        source_manifest_sha256=source_manifest.manifest_sha256,
        vector_artifact_sha256=artifact_authority.artifact.artifact_sha256,
        vector_artifact_file_sha256=artifact_authority.canonical_lf_file_sha256,
    )
    return authority


def validate_vnpy_facade_characterization_authority_fresh_process_v2(
    *,
    receipts: tuple[VnpyFacadeAlgorithmCharacterizationReceiptV2, ...],
    source_manifest: VnpyFacadeSourceManifestV1,
    facade_contract: VnpyFacadeContractV1,
    requirements: tuple[VnpyFacadeCharacterizationRequirementV1, ...],
) -> VnpyFacadeCharacterizationAuthorityV2:
    """Re-execute all five algorithms before accepting durable characterization receipts."""

    from backend.execution_algos.vnpy_compat.facade_characterization import (
        validate_vnpy_facade_characterization_authority_v2,
    )

    artifact_authority = _read_artifact_authority_with_diagnostics_v1()
    execution_sets = run_vnpy_facade_source_execution_sets_v1(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        requirements=requirements,
        artifact_authority=artifact_authority,
    )
    from backend.execution_algos.vnpy_compat.facade_source_execution import (
        build_vnpy_facade_source_executor_binding_v1,
    )

    try:
        binding = build_vnpy_facade_source_executor_binding_v1(
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            vector_artifact_sha256=artifact_authority.artifact.artifact_sha256,
            vector_artifact_file_sha256=artifact_authority.canonical_lf_file_sha256,
        )
        authority = validate_vnpy_facade_characterization_authority_v2(
            receipts=receipts,
            source_manifest=source_manifest,
            facade_contract=facade_contract,
            requirements=requirements,
            ordered_vectors=artifact_authority.artifact.ordered_vectors,
            source_executor_binding=binding,
            source_execution_sets=execution_sets,
        )
    except VnpyFacadeContractError as exc:
        record_vnpy_facade_characterization_build_v1(
            status="FAILED",
            reason_code=stable_exception_reason_code_v1(
                exc,
                default="MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            ),
        )
        raise
    record_vnpy_facade_characterization_build_v1(status="PASSED", reason_code="NONE")
    publish_vnpy_facade_characterization_v1(
        authority=authority,
        source_manifest_sha256=source_manifest.manifest_sha256,
        vector_artifact_sha256=artifact_authority.artifact.artifact_sha256,
        vector_artifact_file_sha256=artifact_authority.canonical_lf_file_sha256,
    )
    return authority


def validate_vnpy_facade_conformance_set_fresh_process_v2(
    *,
    conformance_set: VnpyFacadeConformanceSetV2,
    catalog_runtime: PluginCatalogRuntimeV2,
    gateway_catalog: GatewayCapabilityCatalogV1,
    facade_contract: VnpyFacadeContractV1,
    source_manifest: VnpyFacadeSourceManifestV1,
    requirements: tuple[VnpyFacadeCharacterizationRequirementV1, ...],
) -> VnpyFacadeConformanceAuthorityV2:
    """Rebuild source characterization before validating a durable conformance set."""

    from backend.execution_algos.vnpy_compat.facade_characterization import (
        validate_vnpy_facade_conformance_set_against_authority_v2,
    )

    characterization = build_vnpy_facade_characterization_authority_fresh_process_v2(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        requirements=requirements,
    )
    try:
        authority = validate_vnpy_facade_conformance_set_against_authority_v2(
            conformance_set=conformance_set,
            catalog_runtime=catalog_runtime,
            gateway_catalog=gateway_catalog,
            facade_contract=facade_contract,
            source_manifest=source_manifest,
            characterization_authority_v2=characterization,
        )
    except VnpyFacadeContractError:
        record_vnpy_facade_conformance_v1(
            status="FAILED",
            reason_code="MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID",
        )
        raise
    record_vnpy_facade_conformance_v1(status="PASSED", reason_code="NONE")
    publish_vnpy_facade_conformance_v1(authority)
    return authority


__all__ = [
    "build_vnpy_facade_characterization_authority_fresh_process_v2",
    "run_vnpy_facade_source_execution_sets_v1",
    "validate_vnpy_facade_characterization_authority_fresh_process_v2",
    "validate_vnpy_facade_conformance_set_fresh_process_v2",
    "validate_vnpy_facade_k3_expected_trace_materials_v1",
]
