from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.advisory_phase1 import phase1g_artifact_ref as artifact_ref_module
from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonical_json_text, canonicalize
from backend.services.advisory_phase1.phase1g_artifact_ref import (
    Phase1GArtifactRefError,
    Phase1GArtifactRootBinding,
    Phase1GImmutableArtifactResolver,
    build_phase1g_target_execution_request,
)
from backend.services.advisory_phase1.phase1g_contract import (
    PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY,
    Phase1GInputArtifactKind,
)
from backend.services.advisory_phase1.phase1g_phase1e_projection import Phase1EPlannedOperationType
from backend.services.advisory_phase1.readiness_plan import (
    OperationDisposition,
    Phase1EExecutionPlan,
    PlannedOperationType,
)
from backend.tests.advisory_phase1.phase1g_test_support import (
    h,
    input_ref,
    phase1e_plan,
    raw_sha256,
    release_receipt,
    write_phase1e_plan_artifact,
)


def _resolver(
    *, receipt_root: Path, plan_root: Path, plan_policy_hash: str = h("e")
) -> Phase1GImmutableArtifactResolver:
    return Phase1GImmutableArtifactResolver(
        bindings=(
            Phase1GArtifactRootBinding(
                artifact_kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
                root=receipt_root,
                expected_store_policy_hash=str(PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash),
            ),
            Phase1GArtifactRootBinding(
                artifact_kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
                root=plan_root,
                expected_store_policy_hash=plan_policy_hash,
            ),
        )
    )


def _write_receipt(root: Path) -> tuple[object, bytes]:
    receipt = release_receipt()
    raw = (canonical_json_text(receipt.model_dump(mode="json")) + "\n").encode("utf-8")
    path = root / "receipts" / f"{receipt.receipt_content_hash}.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(raw)
    return receipt, raw


def test_resolver_loads_exact_release_receipt_and_phase1e_plan(tmp_path: Path) -> None:
    receipt_root = tmp_path / "receipts-root"
    plan_root = tmp_path / "plans-root"
    receipt_root.mkdir()
    plan_root.mkdir()
    receipt, receipt_raw = _write_receipt(receipt_root)
    plan = phase1e_plan()
    _, plan_raw = write_phase1e_plan_artifact(root=plan_root, plan=plan, store_policy_hash=h("e"))
    resolver = _resolver(receipt_root=receipt_root, plan_root=plan_root)

    receipt_ref = input_ref(
        kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
        semantic_hash=receipt.receipt_content_hash,
        file_sha256=raw_sha256(receipt_raw),
    )
    plan_ref = input_ref(
        kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
        semantic_hash=str(plan.plan_hash),
        file_sha256=raw_sha256(plan_raw),
        store_policy_hash=h("e"),
    )
    resolved_receipt = resolver.resolve(receipt_ref)
    resolved_plan = resolver.resolve(plan_ref)

    assert resolved_receipt.payload == receipt
    assert resolved_plan.payload.model_dump(mode="json") == plan.model_dump(mode="json")

    request = build_phase1g_target_execution_request(
        target_label=receipt.database_identity.target_label,
        release_schema_receipt_ref=receipt_ref,
        phase1e_plan_ref=plan_ref,
        phase1e_plan=resolved_plan.payload,
        requested_at=datetime(2026, 7, 15, 2, 0, tzinfo=UTC),
    )
    operations = {item.operation_type: item for item in resolved_plan.payload.planned_operations}
    assert request.program_id == plan.scope_key["program_id"]
    assert request.admission_scope_id == plan.evidence_binding.admission_scope_id
    assert request.admission_scope_hash == plan.evidence_binding.admission_scope_hash
    assert (
        request.source_operation_hash == operations[Phase1EPlannedOperationType.SOURCE_RESOLUTION].complete_request_hash
    )
    assert (
        request.observation_template_hash
        == operations[Phase1EPlannedOperationType.OBSERVATION_CAPTURE].request_template_hash
    )


@pytest.mark.parametrize(
    "relative_path",
    (
        "latest.json",
        "receipts/latest.json",
        "../receipts/deadbeef.json",
        "C:/absolute/receipt.json",
        "receipts\\alias.json",
    ),
)
def test_resolver_rejects_alias_escape_and_noncanonical_paths(tmp_path: Path, relative_path: str) -> None:
    receipt_root = tmp_path / "receipts-root"
    plan_root = tmp_path / "plans-root"
    receipt_root.mkdir()
    plan_root.mkdir()
    receipt, receipt_raw = _write_receipt(receipt_root)
    resolver = _resolver(receipt_root=receipt_root, plan_root=plan_root)
    ref = input_ref(
        kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
        semantic_hash=receipt.receipt_content_hash,
        file_sha256=raw_sha256(receipt_raw),
        relative_path=relative_path,
    )

    with pytest.raises(Phase1GArtifactRefError):
        resolver.resolve(ref)


def test_resolver_rejects_file_hash_and_store_policy_mismatch(tmp_path: Path) -> None:
    receipt_root = tmp_path / "receipts-root"
    plan_root = tmp_path / "plans-root"
    receipt_root.mkdir()
    plan_root.mkdir()
    receipt, _ = _write_receipt(receipt_root)
    resolver = _resolver(receipt_root=receipt_root, plan_root=plan_root)

    with pytest.raises(Phase1GArtifactRefError, match="raw file SHA256"):
        resolver.resolve(
            input_ref(
                kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
                semantic_hash=receipt.receipt_content_hash,
                file_sha256=h("f"),
            )
        )

    with pytest.raises(Phase1GArtifactRefError, match="unregistered store policy"):
        Phase1GArtifactRootBinding(
            artifact_kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
            root=receipt_root,
            expected_store_policy_hash=h("0"),
        )

    with pytest.raises(Phase1GArtifactRefError, match="store policy"):
        resolver.resolve(
            input_ref(
                kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
                semantic_hash=receipt.receipt_content_hash,
                file_sha256=h("f"),
                store_policy_hash=h("0"),
            )
        )


def test_phase1e_plan_requires_policy_closure_across_ref_envelope_and_plan(tmp_path: Path) -> None:
    receipt_root = tmp_path / "receipts-root"
    plan_root = tmp_path / "plans-root"
    receipt_root.mkdir()
    plan_root.mkdir()
    plan = phase1e_plan(artifact_store_policy_hash=h("e"))
    _, raw = write_phase1e_plan_artifact(root=plan_root, plan=plan, store_policy_hash=h("f"))
    resolver = _resolver(receipt_root=receipt_root, plan_root=plan_root, plan_policy_hash=h("f"))

    with pytest.raises(Phase1GArtifactRefError, match="semantic closure"):
        resolver.resolve(
            input_ref(
                kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
                semantic_hash=str(plan.plan_hash),
                file_sha256=raw_sha256(raw),
                store_policy_hash=h("f"),
            )
        )


def test_resolver_preserves_exact_deferred_disposition_for_service_classification(
    tmp_path: Path,
) -> None:
    receipt_root = tmp_path / "receipts-root"
    plan_root = tmp_path / "plans-root"
    receipt_root.mkdir()
    plan_root.mkdir()
    payload = phase1e_plan().model_dump(
        mode="python", exclude={"plan_hash", "plan_id"}
    )
    observation = next(
        item
        for item in payload["planned_operations"]
        if item["operation_type"] is PlannedOperationType.OBSERVATION_CAPTURE
    )
    observation["operation_disposition"] = OperationDisposition.DEFERRED
    plan = Phase1EExecutionPlan.model_validate(payload)
    _, raw = write_phase1e_plan_artifact(
        root=plan_root, plan=plan, store_policy_hash=h("e")
    )
    resolver = _resolver(receipt_root=receipt_root, plan_root=plan_root)

    resolved = resolver.resolve(
        input_ref(
            kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
            semantic_hash=str(plan.plan_hash),
            file_sha256=raw_sha256(raw),
        )
    )

    operation = next(
        item
        for item in resolved.payload.planned_operations
        if item.operation_type is Phase1EPlannedOperationType.OBSERVATION_CAPTURE
    )
    assert operation.operation_disposition.value == "DEFERRED"


def _mutate_operation_scope(
    plan: Phase1EExecutionPlan,
    *,
    operation_type: PlannedOperationType,
    mutate,
) -> Phase1EExecutionPlan:  # type: ignore[no-untyped-def]
    payload = plan.model_dump(mode="python", exclude={"plan_hash", "plan_id"})
    for operation in payload["planned_operations"]:
        if operation["operation_type"] != operation_type:
            continue
        request_field = (
            "complete_request_payload"
            if operation["complete_request_payload"] is not None
            else "request_template_payload"
        )
        hash_field = "complete_request_hash" if request_field == "complete_request_payload" else "request_template_hash"
        mutate(operation[request_field]["scope_context"])
        operation[hash_field] = canonical_json_sha256(operation[request_field])
        break
    else:
        raise AssertionError(f"missing operation {operation_type.value}")
    return Phase1EExecutionPlan.model_validate(payload)


@pytest.mark.parametrize(
    ("operation_type", "mutate"),
    (
        (
            PlannedOperationType.OBSERVATION_CAPTURE,
            lambda scope: scope.__setitem__("program_id", "program-b"),
        ),
        (
            PlannedOperationType.OBSERVATION_CAPTURE,
            lambda scope: scope["batch_contract"].pop("artifact_store_policy_hash"),
        ),
    ),
)
def test_phase1e_plan_rejects_operation_scope_or_policy_gap(
    tmp_path: Path,
    operation_type: PlannedOperationType,
    mutate,
) -> None:  # type: ignore[no-untyped-def]
    receipt_root = tmp_path / "receipts-root"
    plan_root = tmp_path / "plans-root"
    receipt_root.mkdir()
    plan_root.mkdir()
    plan = _mutate_operation_scope(phase1e_plan(), operation_type=operation_type, mutate=mutate)
    _, raw = write_phase1e_plan_artifact(root=plan_root, plan=plan, store_policy_hash=h("e"))
    resolver = _resolver(receipt_root=receipt_root, plan_root=plan_root)

    with pytest.raises(Phase1GArtifactRefError, match="semantic closure"):
        resolver.resolve(
            input_ref(
                kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
                semantic_hash=str(plan.plan_hash),
                file_sha256=raw_sha256(raw),
            )
        )


def test_resolver_rejects_phase1e_envelope_and_release_semantic_tamper(tmp_path: Path) -> None:
    receipt_root = tmp_path / "receipts-root"
    plan_root = tmp_path / "plans-root"
    receipt_root.mkdir()
    plan_root.mkdir()
    plan = phase1e_plan()
    plan_path, _ = write_phase1e_plan_artifact(root=plan_root, plan=plan, store_policy_hash=h("e"))
    document = json.loads(plan_path.read_text(encoding="utf-8"))
    document["kind"] = "audit"
    tampered_plan = json.dumps(
        canonicalize(document), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    plan_path.write_bytes(tampered_plan)
    resolver = _resolver(receipt_root=receipt_root, plan_root=plan_root)
    with pytest.raises(Phase1GArtifactRefError, match="envelope identity"):
        resolver.resolve(
            input_ref(
                kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
                semantic_hash=str(plan.plan_hash),
                file_sha256=raw_sha256(tampered_plan),
                store_policy_hash=h("e"),
            )
        )

    receipt = release_receipt()
    raw = (canonical_json_text(receipt.model_dump(mode="json")) + "\n").encode("utf-8")
    wrong_semantic_hash = h("0")
    wrong_path = receipt_root / "receipts" / f"{wrong_semantic_hash}.json"
    wrong_path.parent.mkdir(parents=True)
    wrong_path.write_bytes(raw)
    with pytest.raises(Phase1GArtifactRefError, match="semantic hash"):
        resolver.resolve(
            input_ref(
                kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
                semantic_hash=wrong_semantic_hash,
                file_sha256=raw_sha256(raw),
            )
        )


def test_resolver_requires_both_unique_input_root_bindings(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    receipt_binding = Phase1GArtifactRootBinding(
        artifact_kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
        root=root,
        expected_store_policy_hash=str(PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash),
    )
    with pytest.raises(Phase1GArtifactRefError, match="every Phase 1G input kind"):
        Phase1GImmutableArtifactResolver(bindings=(receipt_binding,))
    with pytest.raises(Phase1GArtifactRefError, match="unique by artifact kind"):
        Phase1GImmutableArtifactResolver(bindings=(receipt_binding, receipt_binding))


def test_resolver_rejects_symlinked_artifact(tmp_path: Path) -> None:
    receipt_root = tmp_path / "receipts-root"
    plan_root = tmp_path / "plans-root"
    outside = tmp_path / "outside"
    receipt_root.mkdir()
    plan_root.mkdir()
    outside.mkdir()
    receipt = release_receipt()
    raw = (canonical_json_text(receipt.model_dump(mode="json")) + "\n").encode("utf-8")
    outside_file = outside / "receipt.json"
    outside_file.write_bytes(raw)
    expected = receipt_root / "receipts" / f"{receipt.receipt_content_hash}.json"
    expected.parent.mkdir()
    try:
        os.symlink(outside_file, expected)
    except OSError:
        pytest.skip("symlink creation is unavailable in this Windows environment")
    resolver = _resolver(receipt_root=receipt_root, plan_root=plan_root)
    with pytest.raises(Phase1GArtifactRefError, match="symlink or reparse"):
        resolver.resolve(
            input_ref(
                kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
                semantic_hash=receipt.receipt_content_hash,
                file_sha256=raw_sha256(raw),
            )
        )


def test_resolver_deterministically_rejects_windows_reparse_attribute(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_root = tmp_path / "receipts-root"
    plan_root = tmp_path / "plans-root"
    receipt_root.mkdir()
    plan_root.mkdir()
    receipt, raw = _write_receipt(receipt_root)
    target = receipt_root / "receipts" / f"{receipt.receipt_content_hash}.json"
    resolver = _resolver(receipt_root=receipt_root, plan_root=plan_root)
    original_lstat = artifact_ref_module.os.lstat

    def fake_lstat(path):  # type: ignore[no-untyped-def]
        observed = original_lstat(path)
        if Path(path) == target:
            return SimpleNamespace(st_mode=observed.st_mode, st_file_attributes=0x0400)
        return observed

    monkeypatch.setattr(artifact_ref_module.os, "lstat", fake_lstat)
    with pytest.raises(Phase1GArtifactRefError, match="symlink or reparse"):
        resolver.resolve(
            input_ref(
                kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
                semantic_hash=receipt.receipt_content_hash,
                file_sha256=raw_sha256(raw),
            )
        )


def test_resolver_rejects_repository_root() -> None:
    with pytest.raises(Phase1GArtifactRefError, match="outside the repository"):
        Phase1GArtifactRootBinding(
            artifact_kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
            root=Path("backend").resolve(),
            expected_store_policy_hash=str(PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash),
        )

    with pytest.raises(Phase1GArtifactRefError, match="absolute path"):
        Phase1GArtifactRootBinding(
            artifact_kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
            root=Path("relative-artifact-root"),
            expected_store_policy_hash=h("e"),
        )

    with pytest.raises(Phase1GArtifactRefError, match="WSL"):
        Phase1GArtifactRootBinding(
            artifact_kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
            root=Path(r"\\wsl$\Ubuntu\home\aistock-artifacts"),
            expected_store_policy_hash=h("e"),
        )
