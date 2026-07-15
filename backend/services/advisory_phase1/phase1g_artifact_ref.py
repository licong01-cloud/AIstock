"""Resolve exact immutable Phase 1G inputs from explicitly bound external roots."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.phase1g_contract import (
    PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY,
    PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY,
    PHASE1G_RESULT_STORE_LAYOUT_POLICY,
    DEFAULT_CAPTURE_POLICY_REGISTRY,
    REASON_INPUT_REF_INVALID,
    REASON_PLAN_INVALID,
    Phase1GContractError,
    Phase1GCapturePolicyRegistry,
    Phase1GInputArtifactKind,
    Phase1GInputArtifactRef,
    Phase1GTargetExecutionRequest,
)
from backend.services.advisory_phase1.readiness_plan import (
    OperationDisposition,
    Phase1EExecutionPlan,
    PlanUnitKind,
    PlannedOperationType,
)
from backend.services.advisory_phase1.release_schema_contract import ReleaseSchemaReceipt, TargetLabel


_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:")
_FILE_ATTRIBUTE_REPARSE_POINT = 0x0400


class Phase1GArtifactRefError(Phase1GContractError):
    def __init__(self, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(REASON_INPUT_REF_INVALID, message, context=context)


@dataclass(frozen=True)
class Phase1GArtifactRootBinding:
    artifact_kind: Phase1GInputArtifactKind
    root: Path
    expected_store_policy_hash: str

    def __post_init__(self) -> None:
        policy_hash = _sha256(self.expected_store_policy_hash, field_name="expected_store_policy_hash")
        root = _validate_external_root(self.root)
        if (
            self.artifact_kind is Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT
            and policy_hash != PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash
        ):
            raise Phase1GArtifactRefError("Phase 1F.2 receipt root is bound to an unregistered store policy")
        object.__setattr__(self, "root", root)
        object.__setattr__(self, "expected_store_policy_hash", policy_hash)


@dataclass(frozen=True)
class ResolvedPhase1GInputArtifact:
    ref: Phase1GInputArtifactRef
    path: Path
    document: dict[str, Any]
    payload: ReleaseSchemaReceipt | Phase1EExecutionPlan


class Phase1GImmutableArtifactResolver:
    """Resolve one exact ref; never search roots or accept a latest alias."""

    def __init__(self, *, bindings: tuple[Phase1GArtifactRootBinding, ...]) -> None:
        by_kind = {binding.artifact_kind: binding for binding in bindings}
        if len(by_kind) != len(bindings):
            raise Phase1GArtifactRefError("artifact roots must be unique by artifact kind")
        expected = set(Phase1GInputArtifactKind)
        if set(by_kind) != expected:
            raise Phase1GArtifactRefError("artifact roots must bind every Phase 1G input kind exactly once")
        self._bindings = by_kind

    def resolve(self, ref: Phase1GInputArtifactRef) -> ResolvedPhase1GInputArtifact:
        binding = self._bindings.get(ref.artifact_kind)
        if binding is None or ref.store_policy_hash != binding.expected_store_policy_hash:
            raise Phase1GArtifactRefError(
                "input ref store policy does not match its exact root binding",
                context={"artifact_kind": ref.artifact_kind.value},
            )
        relative_path = _validate_relative_path(ref)
        path = binding.root / Path(*relative_path.parts)
        _assert_contained(path=path, root=binding.root, stage="before_open")
        _assert_no_reparse_path(path=path, root=binding.root)
        raw = _read_regular_file_without_follow(path=path)
        _assert_contained(path=path, root=binding.root, stage="after_open")
        _assert_no_reparse_path(path=path, root=binding.root)
        if hashlib.sha256(raw).hexdigest() != ref.file_sha256:
            raise Phase1GArtifactRefError("input artifact raw file SHA256 does not match its immutable ref")
        try:
            document = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise Phase1GArtifactRefError("input artifact is not valid UTF-8 JSON") from exc
        if not isinstance(document, dict):
            raise Phase1GArtifactRefError("input artifact must be a JSON object")
        if ref.artifact_kind is Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT:
            payload = self._parse_release_receipt(ref=ref, document=document)
        else:
            payload = self._parse_phase1e_plan(ref=ref, document=document)
        return ResolvedPhase1GInputArtifact(ref=ref, path=path, document=document, payload=payload)

    @staticmethod
    def _parse_release_receipt(*, ref: Phase1GInputArtifactRef, document: dict[str, Any]) -> ReleaseSchemaReceipt:
        try:
            receipt = ReleaseSchemaReceipt.model_validate(document)
        except ValueError as exc:
            raise Phase1GArtifactRefError("Phase 1F.2 release receipt contract is invalid") from exc
        if receipt.receipt_content_hash != ref.semantic_content_hash:
            raise Phase1GArtifactRefError("Phase 1F.2 release receipt semantic hash does not match its ref")
        return receipt

    @staticmethod
    def _parse_phase1e_plan(*, ref: Phase1GInputArtifactRef, document: dict[str, Any]) -> Phase1EExecutionPlan:
        semantic_document = {
            key: value for key, value in document.items() if key not in {"file_sha256", "materialization"}
        }
        if document.get("schema_version") != PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY.envelope_schema_version:
            raise Phase1GArtifactRefError("Phase 1E plan artifact envelope version is invalid")
        if document.get("kind") != "plan" or document.get("identity") != ref.semantic_content_hash:
            raise Phase1GArtifactRefError("Phase 1E plan artifact envelope identity is invalid")
        if document.get("semantic_hash") != ref.semantic_content_hash:
            raise Phase1GArtifactRefError("Phase 1E plan artifact semantic hash does not match its ref")
        if document.get("store_policy_hash") != ref.store_policy_hash:
            raise Phase1GArtifactRefError("Phase 1E plan artifact store policy does not match its ref")
        embedded_file_hash = document.get("file_sha256")
        materialization = document.get("materialization")
        if not isinstance(embedded_file_hash, str) or not isinstance(materialization, dict):
            raise Phase1GArtifactRefError("Phase 1E plan artifact envelope is incomplete")
        if canonical_json_sha256(semantic_document) != embedded_file_hash:
            raise Phase1GArtifactRefError("Phase 1E plan artifact envelope hash is invalid")
        raw_payload = document.get("payload")
        if not isinstance(raw_payload, dict):
            raise Phase1GArtifactRefError("Phase 1E plan artifact payload is missing")
        try:
            plan = Phase1EExecutionPlan.model_validate(raw_payload)
        except ValueError as exc:
            raise Phase1GArtifactRefError("Phase 1E execution plan contract is invalid") from exc
        if plan.plan_unit_kind is not PlanUnitKind.ADMISSION_SCOPE:
            raise Phase1GArtifactRefError("Phase 1G only accepts Phase 1E ADMISSION_SCOPE plans")
        if plan.plan_hash != ref.semantic_content_hash or document.get("identity") != plan.plan_hash:
            raise Phase1GArtifactRefError("Phase 1E execution plan hash does not match its envelope")
        policy_hashes = _phase1e_operation_store_policy_hashes(plan)
        if policy_hashes != {ref.store_policy_hash}:
            raise Phase1GArtifactRefError(
                "Phase 1E plan operations do not bind the exact artifact store policy",
                context={"policy_hash_count": len(policy_hashes)},
            )
        return plan


def build_phase1g_target_execution_request(
    *,
    target_label: TargetLabel,
    release_schema_receipt_ref: Phase1GInputArtifactRef,
    phase1e_plan_ref: Phase1GInputArtifactRef,
    phase1e_plan: Phase1EExecutionPlan,
    requested_at: datetime,
    capture_policy: Phase1GCapturePolicyRegistry = DEFAULT_CAPTURE_POLICY_REGISTRY,
) -> Phase1GTargetExecutionRequest:
    """Derive every Phase 1E semantic field; callers cannot override it."""

    if phase1e_plan.plan_unit_kind is not PlanUnitKind.ADMISSION_SCOPE or phase1e_plan.scope_key is None:
        raise Phase1GContractError(REASON_PLAN_INVALID, "Phase 1G request requires one ADMISSION_SCOPE plan")
    if phase1e_plan_ref.artifact_kind is not Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN:
        raise Phase1GContractError(REASON_PLAN_INVALID, "Phase 1E plan ref has the wrong artifact kind")
    if phase1e_plan.plan_hash != phase1e_plan_ref.semantic_content_hash:
        raise Phase1GContractError(REASON_PLAN_INVALID, "Phase 1E plan ref does not match the loaded plan")
    operations = {item.operation_type: item for item in phase1e_plan.planned_operations}
    source = operations.get(PlannedOperationType.SOURCE_RESOLUTION)
    observation = operations.get(PlannedOperationType.OBSERVATION_CAPTURE)
    if (
        source is None
        or source.operation_disposition is not OperationDisposition.COMPLETE_REQUEST
        or source.complete_request_hash is None
        or observation is None
        or observation.operation_disposition is not OperationDisposition.SEMANTIC_TEMPLATE
        or observation.request_template_hash is None
    ):
        raise Phase1GContractError(
            REASON_PLAN_INVALID,
            "Phase 1E plan does not expose executable source-resolution and observation operations",
        )
    scope_key = phase1e_plan.scope_key
    program_id = str(scope_key.get("program_id") or "").strip()
    admission_scope_id = str(scope_key.get("admission_scope_id") or "").strip()
    raw_date = scope_key.get("decision_trade_date")
    try:
        decision_trade_date = raw_date if isinstance(raw_date, date) else date.fromisoformat(str(raw_date))
    except ValueError as exc:
        raise Phase1GContractError(REASON_PLAN_INVALID, "Phase 1E plan decision trade date is invalid") from exc
    binding = phase1e_plan.evidence_binding
    if (
        not program_id
        or not admission_scope_id
        or admission_scope_id != binding.admission_scope_id
        or binding.admission_scope_hash is None
    ):
        raise Phase1GContractError(REASON_PLAN_INVALID, "Phase 1E plan scope identity is incomplete or inconsistent")
    return Phase1GTargetExecutionRequest(
        target_label=target_label,
        release_schema_receipt_ref=release_schema_receipt_ref,
        phase1e_plan_ref=phase1e_plan_ref,
        phase1e_plan_id=str(phase1e_plan.plan_id),
        phase1e_plan_hash=str(phase1e_plan.plan_hash),
        source_operation_hash=source.complete_request_hash,
        observation_template_hash=observation.request_template_hash,
        program_id=program_id,
        decision_trade_date=decision_trade_date,
        admission_scope_id=admission_scope_id,
        admission_scope_hash=binding.admission_scope_hash,
        capture_policy_registry_id=capture_policy.registry_id,
        capture_policy_registry_version=capture_policy.registry_version,
        capture_policy_registry_hash=str(capture_policy.registry_hash),
        result_store_policy_hash=str(PHASE1G_RESULT_STORE_LAYOUT_POLICY.layout_policy_hash),
        requested_at=requested_at,
    )


def _phase1e_operation_store_policy_hashes(plan: Phase1EExecutionPlan) -> set[str]:
    values: set[str] = set()
    for operation in plan.planned_operations:
        payload = operation.complete_request_payload or operation.request_template_payload
        if not isinstance(payload, Mapping):
            continue
        scope_context = payload.get("scope_context")
        if not isinstance(scope_context, Mapping):
            continue
        batch_contract = scope_context.get("batch_contract")
        if not isinstance(batch_contract, Mapping):
            continue
        raw = batch_contract.get("artifact_store_policy_hash")
        if isinstance(raw, str):
            values.add(_sha256(raw, field_name="artifact_store_policy_hash"))
    return values


def _validate_relative_path(ref: Phase1GInputArtifactRef) -> PurePosixPath:
    raw = ref.relative_path.strip()
    normalized = raw.replace("\\", "/")
    if (
        raw != normalized
        or normalized.startswith("/")
        or normalized.startswith("//")
        or _WINDOWS_DRIVE.match(normalized)
    ):
        raise Phase1GArtifactRefError("input artifact path must be a canonical relative POSIX path")
    relative = PurePosixPath(normalized)
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise Phase1GArtifactRefError("input artifact path contains an invalid segment")
    if any(part.lower() == "latest" or part.lower().startswith("latest.") for part in relative.parts):
        raise Phase1GArtifactRefError("input artifact path cannot use a latest alias")
    if ref.artifact_kind is Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT:
        expected = PurePosixPath("receipts") / f"{ref.semantic_content_hash}.json"
    else:
        expected = (
            PurePosixPath("advisory")
            / "phase1e"
            / "plans"
            / ref.semantic_content_hash[:2]
            / f"{ref.semantic_content_hash}.json"
        )
    if relative != expected:
        raise Phase1GArtifactRefError(
            "input artifact path does not match its registered content-addressed layout",
            context={"artifact_kind": ref.artifact_kind.value},
        )
    return relative


def _read_regular_file_without_follow(*, path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise Phase1GArtifactRefError(
            "input artifact cannot be opened as an immutable file",
            context={"errno": exc.errno},
        ) from exc
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise Phase1GArtifactRefError("input artifact is not a regular file")
        current = os.stat(path, follow_symlinks=False)
        if (opened.st_dev, opened.st_ino, opened.st_size) != (current.st_dev, current.st_ino, current.st_size):
            raise Phase1GArtifactRefError("input artifact changed while it was being opened")
        with open(descriptor, "rb", closefd=False) as stream:
            return stream.read()
    finally:
        os.close(descriptor)


def _assert_contained(*, path: Path, root: Path, stage: str) -> None:
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise Phase1GArtifactRefError(
            "input artifact path escapes its exact root",
            context={"stage": stage},
        ) from exc


def _assert_no_reparse_path(*, path: Path, root: Path) -> None:
    relative = path.relative_to(root)
    current = root
    for part in relative.parts:
        current = current / part
        try:
            attributes = os.lstat(current)
        except OSError as exc:
            raise Phase1GArtifactRefError("input artifact path component is unavailable") from exc
        if stat.S_ISLNK(attributes.st_mode) or (
            getattr(attributes, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT
        ):
            raise Phase1GArtifactRefError("input artifact path contains a symlink or reparse point")


def _validate_external_root(root: Path) -> Path:
    raw = str(root.expanduser()).replace("\\", "/")
    normalized = raw.lower()
    if not root.expanduser().is_absolute():
        raise Phase1GArtifactRefError("artifact root must be an explicit absolute path")
    if normalized.startswith("//wsl$/") or normalized.startswith("//wsl.localhost/") or normalized.startswith("/mnt/"):
        raise Phase1GArtifactRefError("artifact root cannot be a WSL filesystem path")
    unresolved = root.expanduser()
    _assert_root_chain_has_no_reparse(unresolved)
    try:
        resolved = unresolved.resolve(strict=True)
    except OSError as exc:
        raise Phase1GArtifactRefError("artifact root cannot be resolved") from exc
    if not resolved.is_dir():
        raise Phase1GArtifactRefError("artifact root must be an existing directory")
    repository_root = Path(__file__).resolve().parents[3]
    try:
        resolved.relative_to(repository_root)
    except ValueError:
        pass
    else:
        raise Phase1GArtifactRefError("artifact root must be outside the repository")
    return resolved


def _assert_root_chain_has_no_reparse(path: Path) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        if not current.exists():
            raise Phase1GArtifactRefError("artifact root must already exist")
        attributes = os.lstat(current)
        if stat.S_ISLNK(attributes.st_mode) or (
            getattr(attributes, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT
        ):
            raise Phase1GArtifactRefError("artifact root cannot traverse a symlink or reparse point")


def _sha256(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise Phase1GArtifactRefError(f"{field_name} must be lowercase sha256")
    return normalized
