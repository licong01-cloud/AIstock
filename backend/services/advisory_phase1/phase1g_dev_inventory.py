"""Read-only DEV input inventory for Phase 1G G5."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.phase1g_artifact_ref import (
    build_phase1g_target_execution_request,
)
from backend.services.advisory_phase1.phase1g_command_factory import Phase1GCommandContext
from backend.services.advisory_phase1.phase1g_contract import (
    PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY,
    PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY,
    Phase1GExecutionBatchRequest,
    Phase1GInputArtifactKind,
    Phase1GInputArtifactRef,
    REASON_TARGET_DIAGNOSTIC,
)
from backend.services.advisory_phase1.phase1g_phase1e_projection import (
    Phase1EExecutionPlanProjection,
    Phase1EOperationDisposition,
    Phase1EPlannedOperationType,
    Phase1EPlanUnitKind,
)
from backend.services.advisory_phase1.phase1g_schema_guard import Phase1GReleaseSchemaGuard
from backend.services.advisory_phase1.release_schema_contract import ReleaseSchemaReceipt, TargetLabel
from backend.services.advisory_phase1.source_resolution import SourceResolutionReceipt

from .phase1g_dev_evidence_contract import (
    AlphaMode,
    InventoryStatus,
    L3SourceClassification,
    L4TargetClassification,
    Phase1GDevEvidenceError,
    Phase1GDevIdentityHashRef,
    Phase1GDevInputInventoryReceipt,
    Phase1GDevL3SourceCandidate,
    Phase1GDevL4TargetCandidate,
    REASON_INVENTORY_INVALID,
    REASON_L3_SOURCE_PENDING,
    REASON_MULTI_TRACK_MISSING,
    REASON_REAL_INPUT_PENDING,
    REASON_SINGLE_TRACK_MISSING,
    REASON_UNEXPECTED_ERROR,
)


_FILE_ATTRIBUTE_REPARSE_POINT = 0x0400
NowProvider = Callable[[], datetime]


class Phase1GDevInventory:
    def __init__(
        self,
        *,
        context: Phase1GCommandContext,
        release_receipt_root: Path,
        phase1e_artifact_root: Path,
        now_provider: NowProvider | None = None,
    ) -> None:
        if context.connection_config.target_label is not TargetLabel.DEV:
            raise Phase1GDevEvidenceError(
                REASON_INVENTORY_INVALID,
                "G5 inventory requires the exact DEV target",
            )
        self._context = context
        self._release_root = release_receipt_root.expanduser().resolve(strict=True)
        self._phase1e_root = phase1e_artifact_root.expanduser().resolve(strict=True)
        self._now = now_provider or (lambda: datetime.now(timezone.utc))
        self._schema_guard = Phase1GReleaseSchemaGuard()
        self._diagnostic_reasons: set[str] = set()

    def run(self) -> Phase1GDevInputInventoryReceipt:
        observed_at = self._aware_now()
        releases = self._compatible_release_receipts()
        if not releases:
            raise Phase1GDevEvidenceError(
                REASON_INVENTORY_INVALID,
                "no exact downstream-ready Phase 1F.2 DEV receipt matches the current catalog",
            )
        release_refs = tuple(item[0] for item in releases)
        catalog_fingerprints = {item[2] for item in releases}
        identities = {item[3].model_dump_json() for item in releases}
        if len(catalog_fingerprints) != 1 or len(identities) != 1:
            raise Phase1GDevEvidenceError(
                REASON_INVENTORY_INVALID,
                "compatible release receipts disagree on current DEV identity or catalog",
            )

        l3: list[Phase1GDevL3SourceCandidate] = []
        l4: list[Phase1GDevL4TargetCandidate] = []
        inventory_reasons: set[str] = set(self._diagnostic_reasons)
        for plan_ref in self._phase1e_plan_refs():
            try:
                resolved = self._context.artifact_resolver.resolve(plan_ref)
                plan = resolved.payload
                if not isinstance(plan, Phase1EExecutionPlanProjection):
                    raise ValueError("Phase 1E artifact resolved to the wrong payload type")
            except Exception as exc:  # Invalid artifacts are reported and never skipped as success.
                inventory_reasons.add(
                    _inventory_reason(
                        exc,
                        default=REASON_INVENTORY_INVALID,
                        operation="resolve_phase1e_plan",
                    )
                )
                continue
            for release_ref, _receipt, _catalog, _identity in releases:
                if plan.plan_unit_kind is Phase1EPlanUnitKind.ADMISSION_SCOPE:
                    l3.append(
                        self._build_l3_source(
                            plan=plan,
                            plan_ref=plan_ref,
                            release_ref=release_ref,
                            observed_at=observed_at,
                        )
                    )
                target = self._build_l4_target(
                    plan=plan,
                    plan_ref=plan_ref,
                    release_ref=release_ref,
                    observed_at=observed_at,
                )
                if target is not None:
                    l4.append(target)

        l3 = _unique_l3(l3)
        l4 = _unique_l4(l4)
        inventory_reasons.update(self._diagnostic_reasons)
        l3_count = sum(item.classification is not L3SourceClassification.INCOMPLETE for item in l3)
        single_count = sum(item.classification is L4TargetClassification.EXECUTABLE_SINGLE for item in l4)
        multi_count = sum(item.classification is L4TargetClassification.EXECUTABLE_NATIVE_MULTI for item in l4)
        if single_count and multi_count:
            status = InventoryStatus.L4_DUAL_TRACK_READY
        elif l3_count:
            status = InventoryStatus.L3_READY_L4_PENDING
            inventory_reasons.add(REASON_REAL_INPUT_PENDING)
            if not single_count:
                inventory_reasons.add(REASON_SINGLE_TRACK_MISSING)
            if not multi_count:
                inventory_reasons.add(REASON_MULTI_TRACK_MISSING)
        else:
            status = InventoryStatus.L3_SOURCE_PENDING
            inventory_reasons.update({REASON_L3_SOURCE_PENDING, REASON_REAL_INPUT_PENDING})
            if not single_count:
                inventory_reasons.add(REASON_SINGLE_TRACK_MISSING)
            if not multi_count:
                inventory_reasons.add(REASON_MULTI_TRACK_MISSING)

        return Phase1GDevInputInventoryReceipt(
            inventory_invocation_id=f"p1g_g5_inv_{uuid4().hex}",
            database_identity=releases[0][3],
            release_receipt_refs=release_refs,
            catalog_fingerprint=releases[0][2],
            artifact_root_policy_hashes=(
                str(PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash),
                str(PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY.layout_policy_hash),
            ),
            l3_source_candidates=tuple(l3),
            l4_target_candidates=tuple(l4),
            l3_source_set_hash=_set_hash(item.source_candidate_hash for item in l3),
            l4_target_set_hash=_set_hash(item.target_candidate_hash for item in l4),
            l3_source_eligible_count=l3_count,
            l4_single_executable_count=single_count,
            l4_native_multi_executable_count=multi_count,
            inventory_status=status,
            reason_codes=tuple(sorted(inventory_reasons)),
            observed_at=observed_at,
        )

    def _compatible_release_receipts(
        self,
    ) -> list[tuple[Phase1GInputArtifactRef, ReleaseSchemaReceipt, str, Any]]:
        candidates: list[tuple[Phase1GInputArtifactRef, ReleaseSchemaReceipt, str, Any]] = []
        receipts_dir = self._release_root / "receipts"
        for path in _regular_json_files(receipts_dir):
            raw = path.read_bytes()
            try:
                document = json.loads(raw.decode("utf-8"))
                receipt = ReleaseSchemaReceipt.model_validate(document)
                ref = Phase1GInputArtifactRef(
                    artifact_kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
                    store_policy_hash=str(
                        PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash
                    ),
                    relative_path=path.relative_to(self._release_root).as_posix(),
                    semantic_content_hash=str(receipt.receipt_content_hash),
                    file_sha256=hashlib.sha256(raw).hexdigest(),
                )
                resolved = self._context.artifact_resolver.resolve(ref)
                if not isinstance(resolved.payload, ReleaseSchemaReceipt):
                    continue
                evidence = self._schema_guard.verify(
                    receipt=resolved.payload,
                    target_label=TargetLabel.DEV,
                    connection_config=self._context.connection_config,
                )
            except Exception as exc:
                self._diagnostic_reasons.add(
                    _inventory_reason(
                        exc,
                        default=REASON_INVENTORY_INVALID,
                        operation="verify_release_receipt",
                    )
                )
                continue
            candidates.append(
                (ref, resolved.payload, evidence.catalog_fingerprint, evidence.database_identity)
            )
        return sorted(candidates, key=lambda item: item[0].semantic_content_hash)

    def _phase1e_plan_refs(self) -> tuple[Phase1GInputArtifactRef, ...]:
        refs: list[Phase1GInputArtifactRef] = []
        plans_root = self._phase1e_root / "advisory" / "phase1e" / "plans"
        for path in _two_level_json_files(plans_root):
            raw = path.read_bytes()
            try:
                document = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._diagnostic_reasons.add(REASON_INVENTORY_INVALID)
                continue
            identity = document.get("semantic_hash")
            if not isinstance(identity, str):
                self._diagnostic_reasons.add(REASON_INVENTORY_INVALID)
                continue
            try:
                refs.append(
                    Phase1GInputArtifactRef(
                        artifact_kind=Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN,
                        store_policy_hash=str(
                            PHASE1E_EXECUTION_PLAN_LAYOUT_POLICY.layout_policy_hash
                        ),
                        relative_path=path.relative_to(self._phase1e_root).as_posix(),
                        semantic_content_hash=identity,
                        file_sha256=hashlib.sha256(raw).hexdigest(),
                    )
                )
            except ValueError:
                self._diagnostic_reasons.add(REASON_INVENTORY_INVALID)
                continue
        return tuple(sorted(refs, key=lambda item: item.semantic_content_hash))

    def _build_l3_source(
        self,
        *,
        plan: Phase1EExecutionPlanProjection,
        plan_ref: Phase1GInputArtifactRef,
        release_ref: Phase1GInputArtifactRef,
        observed_at: datetime,
    ) -> Phase1GDevL3SourceCandidate:
        binding = plan.evidence_binding
        source = _operation(plan, Phase1EPlannedOperationType.SOURCE_RESOLUTION)
        observation = _operation(
            plan, Phase1EPlannedOperationType.OBSERVATION_CAPTURE
        )
        alpha_mode = AlphaMode(binding.alpha_mode)
        failure_reasons: set[str] = set(plan.reason_codes)
        receipt: SourceResolutionReceipt | None = None
        events: tuple[Phase1GDevIdentityHashRef, ...] = ()
        if source is None or source.complete_request_payload is None:
            failure_reasons.add(REASON_L3_SOURCE_PENDING)
        else:
            receipt_payload = source.complete_request_payload.get(
                "source_resolution_receipt"
            )
            if isinstance(receipt_payload, dict):
                try:
                    receipt = SourceResolutionReceipt.model_validate(receipt_payload)
                except ValueError:
                    failure_reasons.add(REASON_L3_SOURCE_PENDING)
            else:
                failure_reasons.add(REASON_L3_SOURCE_PENDING)
        if receipt is not None and receipt.source_resolution_receipt_hash is not None:
            events = tuple(
                Phase1GDevIdentityHashRef(
                    identity=str(item.selected_availability_event_hash),
                    content_hash=str(item.selected_availability_event_hash),
                )
                for item in receipt.requirement_resolutions
                if item.selected_availability_event_hash is not None
            )
        else:
            failure_reasons.add(REASON_L3_SOURCE_PENDING)
        semantic_source_ready = (
            receipt is not None
            and receipt.source_resolution_receipt_hash is not None
            and source is not None
            and source.operation_disposition
            is Phase1EOperationDisposition.COMPLETE_REQUEST
            and observation is not None
            and observation.operation_disposition
            is Phase1EOperationDisposition.SEMANTIC_TEMPLATE
            and bool(events)
            and receipt.can_create_capture_plan
        )
        eligible = False
        if semantic_source_ready:
            try:
                request = build_phase1g_target_execution_request(
                    target_label=TargetLabel.DEV,
                    release_schema_receipt_ref=release_ref,
                    phase1e_plan_ref=plan_ref,
                    phase1e_plan=plan,
                    requested_at=observed_at,
                )
                target_plan = self._context.service.plan_batch(
                    Phase1GExecutionBatchRequest(targets=(request,))
                ).target_plans[0]
                events = tuple(
                    Phase1GDevIdentityHashRef(
                        identity=item.identity,
                        content_hash=item.content_hash,
                    )
                    for item in target_plan.expected_source_events
                )
                eligible = (
                    target_plan.observed_capture_batch_state_hash
                    == canonical_json_sha256([])
                    and not target_plan.observed_outbox_identity_hashes
                )
                if not eligible:
                    failure_reasons.add(REASON_L3_SOURCE_PENDING)
            except Exception as exc:
                failure_reasons.add(
                    _inventory_reason(
                        exc,
                        default=REASON_L3_SOURCE_PENDING,
                        operation="plan_l3_source",
                    )
                )
        else:
            failure_reasons.add(REASON_L3_SOURCE_PENDING)
        classification = (
            L3SourceClassification.ELIGIBLE_SINGLE
            if eligible and alpha_mode is AlphaMode.SINGLE
            else L3SourceClassification.ELIGIBLE_NATIVE_MULTI
            if eligible
            else L3SourceClassification.INCOMPLETE
        )
        reasons = () if eligible else tuple(sorted(failure_reasons))
        return Phase1GDevL3SourceCandidate(
            source_phase1e_plan_ref=plan_ref,
            release_receipt_ref=release_ref,
            alpha_mode=alpha_mode,
            component_package_ids=tuple(binding.manifest_alpha_component_ids),
            decision_trade_date=plan.decision_trade_date,
            package_id=binding.package_id,
            manifest_sha256=binding.manifest_sha256,
            selection_evidence=Phase1GDevIdentityHashRef(
                identity=binding.selection_evidence_id,
                content_hash=binding.selection_evidence_hash,
            ),
            selection_artifact=Phase1GDevIdentityHashRef(
                identity=binding.selection_artifact_id,
                content_hash=binding.selection_artifact_payload_hash,
            ),
            source_resolution_receipt_hash=(
                str(receipt.source_resolution_receipt_hash)
                if receipt is not None
                and receipt.source_resolution_receipt_hash is not None
                else None
            ),
            source_event_refs=events,
            classification=classification,
            reason_codes=reasons,
        )

    def _build_l4_target(
        self,
        *,
        plan: Phase1EExecutionPlanProjection,
        plan_ref: Phase1GInputArtifactRef,
        release_ref: Phase1GInputArtifactRef,
        observed_at: datetime,
    ) -> Phase1GDevL4TargetCandidate | None:
        binding = plan.evidence_binding
        alpha_mode = AlphaMode(binding.alpha_mode)
        source_events = _source_event_refs(plan)
        if plan.plan_unit_kind is Phase1EPlanUnitKind.TARGET_DIAGNOSTIC:
            target = plan.target_key or {}
            program_id = str(target.get("program_id", "")).strip()
            if not program_id:
                self._diagnostic_reasons.add(REASON_INVENTORY_INVALID)
                return None
            return Phase1GDevL4TargetCandidate(
                alpha_mode=alpha_mode,
                component_package_ids=tuple(binding.manifest_alpha_component_ids),
                decision_trade_date=plan.decision_trade_date,
                program_id=program_id,
                package_id=binding.package_id,
                manifest_sha256=binding.manifest_sha256,
                phase1e_plan_ref=plan_ref,
                dse=Phase1GDevIdentityHashRef(
                    identity=binding.selection_evidence_id,
                    content_hash=binding.selection_evidence_hash,
                ),
                selection_artifact=Phase1GDevIdentityHashRef(
                    identity=binding.selection_artifact_id,
                    content_hash=binding.selection_artifact_payload_hash,
                ),
                source_event_refs=source_events,
                classification=L4TargetClassification.DIAGNOSTIC,
                reason_codes=tuple(
                    sorted(set(plan.reason_codes) | {REASON_TARGET_DIAGNOSTIC})
                ),
            )
        scope = plan.scope_key
        if (
            scope is None
            or binding.admission_scope_id is None
            or binding.admission_scope_hash is None
        ):
            self._diagnostic_reasons.add(REASON_INVENTORY_INVALID)
            return None
        classification = L4TargetClassification.DEFERRED
        reasons: tuple[str, ...] = tuple(plan.reason_codes) or (REASON_REAL_INPUT_PENDING,)
        target_request = None
        try:
            target_request = build_phase1g_target_execution_request(
                target_label=TargetLabel.DEV,
                release_schema_receipt_ref=release_ref,
                phase1e_plan_ref=plan_ref,
                phase1e_plan=plan,
                requested_at=observed_at,
            )
            target_plan = self._context.service.plan_batch(
                Phase1GExecutionBatchRequest(targets=(target_request,))
            ).target_plans[0]
            source_events = tuple(
                Phase1GDevIdentityHashRef(
                    identity=item.identity,
                    content_hash=item.content_hash,
                )
                for item in target_plan.expected_source_events
            )
            classification = (
                L4TargetClassification.EXECUTABLE_SINGLE
                if alpha_mode is AlphaMode.SINGLE
                else L4TargetClassification.EXECUTABLE_NATIVE_MULTI
            )
            reasons = ()
        except Exception as exc:
            reason = _inventory_reason(
                exc,
                default=REASON_REAL_INPUT_PENDING,
                operation="plan_l4_target",
            )
            classification = (
                L4TargetClassification.DEFERRED
                if any(
                    item.operation_disposition is Phase1EOperationDisposition.DEFERRED
                    for item in plan.planned_operations
                )
                else L4TargetClassification.STALE
            )
            reasons = tuple(sorted(set(plan.reason_codes) | {reason}))
        return Phase1GDevL4TargetCandidate(
            target_request=target_request,
            alpha_mode=alpha_mode,
            component_package_ids=tuple(binding.manifest_alpha_component_ids),
            decision_trade_date=plan.decision_trade_date,
            program_id=str(scope["program_id"]),
            package_id=binding.package_id,
            manifest_sha256=binding.manifest_sha256,
            admission_scope_id=binding.admission_scope_id,
            admission_scope_hash=binding.admission_scope_hash,
            phase1e_plan_ref=plan_ref,
            dse=Phase1GDevIdentityHashRef(
                identity=binding.selection_evidence_id,
                content_hash=binding.selection_evidence_hash,
            ),
            selection_artifact=Phase1GDevIdentityHashRef(
                identity=binding.selection_artifact_id,
                content_hash=binding.selection_artifact_payload_hash,
            ),
            source_event_refs=source_events,
            classification=classification,
            reason_codes=reasons,
        )

    def _aware_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise Phase1GDevEvidenceError(
                REASON_INVENTORY_INVALID,
                "inventory clock must be timezone-aware",
            )
        return value.astimezone(timezone.utc)


def _operation(
    plan: Phase1EExecutionPlanProjection,
    operation_type: Phase1EPlannedOperationType,
):  # type: ignore[no-untyped-def]
    return next(
        (item for item in plan.planned_operations if item.operation_type is operation_type),
        None,
    )


def _source_event_refs(plan: Phase1EExecutionPlanProjection) -> tuple[Phase1GDevIdentityHashRef, ...]:
    source = _operation(plan, Phase1EPlannedOperationType.SOURCE_RESOLUTION)
    if source is None or not isinstance(source.complete_request_payload, dict):
        return ()
    payload = source.complete_request_payload.get("source_resolution_receipt")
    if not isinstance(payload, dict):
        return ()
    try:
        receipt = SourceResolutionReceipt.model_validate(payload)
    except ValueError:
        return ()
    return tuple(
        Phase1GDevIdentityHashRef(
            identity=str(item.selected_availability_event_hash),
            content_hash=str(item.selected_availability_event_hash),
        )
        for item in receipt.requirement_resolutions
        if item.selected_availability_event_hash is not None
    )


def _inventory_reason(
    exc: Exception,
    *,
    default: str,
    operation: str,
) -> str:
    supplied = getattr(exc, "reason_code", None)
    if supplied is None and not isinstance(exc, (ValueError, OSError)):
        raise Phase1GDevEvidenceError(
            REASON_UNEXPECTED_ERROR,
            "unexpected DEV inventory failure",
            context={
                "exception_type": type(exc).__name__,
                "operation": operation,
            },
        ) from exc
    value = str(supplied or default)
    return value if len(value) <= 160 else default


def _set_hash(values) -> str:  # type: ignore[no-untyped-def]
    return hashlib.sha256(
        json.dumps(sorted(str(value) for value in values), separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _unique_l3(values: list[Phase1GDevL3SourceCandidate]) -> list[Phase1GDevL3SourceCandidate]:
    return [
        item
        for _, item in sorted(
            {str(item.source_candidate_hash): item for item in values}.items()
        )
    ]


def _unique_l4(values: list[Phase1GDevL4TargetCandidate]) -> list[Phase1GDevL4TargetCandidate]:
    return [
        item
        for _, item in sorted(
            {str(item.target_candidate_hash): item for item in values}.items()
        )
    ]


def _regular_json_files(root: Path) -> tuple[Path, ...]:
    if not root.is_dir() or _is_reparse(root):
        return ()
    files: list[Path] = []
    with os.scandir(root) as entries:
        for entry in entries:
            if not entry.name.endswith(".json") or not entry.is_file(follow_symlinks=False):
                continue
            path = Path(entry.path)
            if not _is_reparse(path):
                files.append(path)
    return tuple(sorted(files, key=lambda item: item.name))


def _two_level_json_files(root: Path) -> tuple[Path, ...]:
    if not root.is_dir() or _is_reparse(root):
        return ()
    files: list[Path] = []
    with os.scandir(root) as prefixes:
        for prefix in prefixes:
            prefix_path = Path(prefix.path)
            if not prefix.is_dir(follow_symlinks=False) or _is_reparse(prefix_path):
                continue
            files.extend(_regular_json_files(prefix_path))
    return tuple(sorted(files, key=lambda item: item.as_posix()))


def _is_reparse(path: Path) -> bool:
    attributes = os.lstat(path)
    return stat.S_ISLNK(attributes.st_mode) or bool(
        getattr(attributes, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT
    )
