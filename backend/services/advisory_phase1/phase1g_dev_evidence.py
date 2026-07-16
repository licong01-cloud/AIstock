"""Phase 1G G5 DEV evidence orchestration."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from pathlib import Path
import shutil
import tempfile
import traceback
from typing import Any, Callable
from uuid import uuid4

import psycopg2
import psycopg2.extras

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.control_binding import (
    ControlBindingRequest,
    PostgresControlBindingRepository,
    REASON_CONTROL_BINDING_UNAVAILABLE,
)
from backend.services.advisory_phase1.phase1g_command_factory import (
    Phase1GCommandContext,
    build_phase1g_command_context,
    verify_phase1g_attempt_database,
)
from backend.services.advisory_phase1.phase1g_contract import (
    DEFAULT_CAPTURE_POLICY_REGISTRY,
    Phase1GAttemptReceipt,
    Phase1GBatchAttemptReceipt,
    Phase1GExecutionBatchPlan,
    Phase1GExecutionBatchRequest,
    REASON_PLAN_STALE,
)
from backend.services.advisory_phase1.phase1g_service import (
    Phase1GInvocationBatchStatus,
    Phase1GOperationStatus,
    Phase1GService,
)
from backend.services.advisory_phase1.release_schema_contract import TargetLabel
from backend.services.advisory_phase1.source_ledger import SourceLedgerError

from .phase1g_dev_evidence_contract import (
    AlphaMode,
    EvidenceKind,
    ExecutionMode,
    G5_DATABASE_WRITE_PHASES,
    InventoryStatus,
    L3SourceClassification,
    PersistentStatus,
    Phase1GDevEvidenceError,
    Phase1GDevEvidenceRef,
    Phase1GDevEvidenceSummary,
    Phase1GDevExecutionManifest,
    Phase1GDevInputInventoryReceipt,
    Phase1GDevPersistentReceipt,
    Phase1GDevPersistentTargetOutcome,
    Phase1GDevRollbackReceipt,
    REASON_EVIDENCE_STORE_FAILED,
    REASON_L3_ROLLBACK_FAILED,
    REASON_L3_SOURCE_PENDING,
    REASON_L4_PLAN_STALE,
    REASON_L4_PARTIAL_FAILURE,
    REASON_MANIFEST_INVALID,
    REASON_REAL_INPUT_PENDING,
    REASON_REFERENCED_READBACK_FAILED,
    REASON_UNEXPECTED_ERROR,
    RollbackStatus,
    SummaryStatus,
)
from .phase1g_dev_evidence_postgres import (
    capture_current_transaction_residue_probes,
    run_control_binding_concurrency_probe,
    verify_zero_residue,
)
from .phase1g_dev_evidence_store import Phase1GDevEvidenceStore, StoredG5Evidence
from .phase1g_dev_inventory import Phase1GDevInventory
from .phase1g_dev_rollback import Phase1GDevRollbackCoordinator
from .phase1g_l3_validation_evidence import Phase1GL3ValidationEvidenceComposer


LOGGER = logging.getLogger(__name__)
NowProvider = Callable[[], datetime]


class _PreloadedRollbackPhase1GService(Phase1GService):
    """Run G4 writes from immutable targets frozen before the owner transaction."""

    def __init__(self, *, preloaded_targets: dict[str, Any], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._preloaded_targets = preloaded_targets

    def _load_target(self, target):  # type: ignore[no-untyped-def]
        loaded = self._preloaded_targets.get(str(target.request_hash))
        if loaded is None or loaded.target_request != target:
            raise Phase1GDevEvidenceError(
                REASON_MANIFEST_INVALID,
                "rollback target differs from its frozen immutable preload",
            )
        return loaded


def _log_sanitized_exception(
    message: str, exc: Exception, *args: object
) -> None:
    frames = " > ".join(
        f"{Path(frame.filename).name}:{frame.lineno}:{frame.name}"
        for frame in traceback.extract_tb(exc.__traceback__)
    )
    LOGGER.error(
        message + " exception_type=%s redacted_traceback=%s",
        *args,
        type(exc).__name__,
        frames,
    )


def _persistent_reason_set(*reason_groups: tuple[str, ...]) -> set[str]:
    reasons = {reason for group in reason_groups for reason in group}
    if REASON_PLAN_STALE in reasons:
        reasons.add(REASON_L4_PLAN_STALE)
    return reasons


def _persistent_summary_status(status: PersistentStatus) -> SummaryStatus:
    if status is PersistentStatus.COMPLETE_DUAL_TRACK:
        return SummaryStatus.COMPLETE
    if status is PersistentStatus.PARTIAL_FAILURE:
        return SummaryStatus.PARTIAL_FAILURE
    return SummaryStatus.FAILED


def _build_persistent_target_outcomes(
    *,
    candidates,  # type: ignore[no-untyped-def]
    first,  # type: ignore[no-untyped-def]
    rerun,  # type: ignore[no-untyped-def]
    global_reasons: set[str],
) -> tuple[Phase1GDevPersistentTargetOutcome, ...]:
    first_by_target = (
        {item.target_request_hash: item for item in first.target_outcomes}
        if first is not None
        else {}
    )
    rerun_by_target = (
        {item.target_request_hash: item for item in rerun.target_outcomes}
        if rerun is not None
        else {}
    )
    outcomes: list[Phase1GDevPersistentTargetOutcome] = []
    for candidate in candidates:
        if candidate.target_request is None:
            raise Phase1GDevEvidenceError(
                REASON_MANIFEST_INVALID,
                "persistent candidate is missing its target request",
            )
        request_hash = str(candidate.target_request.request_hash)
        first_target = first_by_target.get(request_hash)
        rerun_target = rerun_by_target.get(request_hash)
        target_reasons = _persistent_reason_set(
            tuple(first_target.reason_codes) if first_target is not None else (),
            tuple(rerun_target.reason_codes) if rerun_target is not None else (),
            tuple(global_reasons),
        )
        first_ref = (
            first_target.attempt_receipt_ref if first_target is not None else None
        )
        rerun_ref = (
            rerun_target.attempt_receipt_ref if rerun_target is not None else None
        )
        exact = bool(
            first_target is not None
            and rerun_target is not None
            and first_target.operation_status is Phase1GOperationStatus.SUCCESS
            and rerun_target.operation_status is Phase1GOperationStatus.SUCCESS
            and not rerun_target.dml_executed
            and not (
                set(rerun_target.committed_phases) & G5_DATABASE_WRITE_PHASES
            )
            and first_target.capture_result_hash is not None
            and first_target.capture_result_hash
            == rerun_target.capture_result_hash
            and first_ref is not None
            and rerun_ref is not None
            and first_ref.semantic_content_hash != rerun_ref.semantic_content_hash
            and not target_reasons
        )
        if not exact:
            target_reasons.add(REASON_L4_PARTIAL_FAILURE)
        stable_result_hash = (
            first_target.capture_result_hash
            if first_target is not None
            else rerun_target.capture_result_hash
            if rerun_target is not None
            else None
        )
        outcomes.append(
            Phase1GDevPersistentTargetOutcome(
                target_request_hash=request_hash,
                alpha_mode=AlphaMode(candidate.alpha_mode),
                first_operation_status=(
                    first_target.operation_status.value
                    if first_target is not None
                    else "NOT_RUN"
                ),
                rerun_operation_status=(
                    rerun_target.operation_status.value
                    if rerun_target is not None
                    else None
                ),
                first_dml_executed=(
                    first_target.dml_executed if first_target is not None else False
                ),
                rerun_dml_executed=(
                    rerun_target.dml_executed if rerun_target is not None else None
                ),
                first_committed_phases=(
                    first_target.committed_phases if first_target is not None else ()
                ),
                rerun_committed_phases=(
                    rerun_target.committed_phases if rerun_target is not None else ()
                ),
                stable_result_hash=stable_result_hash,
                first_attempt_ref=first_ref,
                rerun_attempt_ref=rerun_ref,
                exact_rerun_verified=exact,
                reason_codes=tuple(sorted(target_reasons)),
            )
        )
    return tuple(outcomes)


def verify_g5_reference_closure(
    *, store: Phase1GDevEvidenceStore, ref: Phase1GDevEvidenceRef
) -> tuple[Any, str]:
    model = store.load(ref)
    evidence: list[dict[str, str]] = [
        {
            "kind": ref.evidence_kind.value,
            "hash": ref.semantic_content_hash,
        }
    ]

    def load_ref(
        child_ref: Phase1GDevEvidenceRef,
        expected_type: type,
    ) -> Any:
        try:
            child = store.load(child_ref)
        except Phase1GDevEvidenceError as exc:
            raise Phase1GDevEvidenceError(
                REASON_REFERENCED_READBACK_FAILED,
                "G5 referenced evidence is unavailable or invalid",
                context={"evidence_kind": child_ref.evidence_kind.value},
            ) from exc
        if not isinstance(child, expected_type):
            raise Phase1GDevEvidenceError(
                REASON_REFERENCED_READBACK_FAILED,
                "G5 referenced evidence has the wrong contract type",
            )
        evidence.append(
            {
                "kind": child_ref.evidence_kind.value,
                "hash": child_ref.semantic_content_hash,
            }
        )
        return child

    def load_manifest(identity: str) -> Phase1GDevExecutionManifest:
        try:
            child = store.load_by_identity(
                kind=EvidenceKind.MANIFEST,
                identity=identity,
            )
        except Phase1GDevEvidenceError as exc:
            raise Phase1GDevEvidenceError(
                REASON_REFERENCED_READBACK_FAILED,
                "G5 referenced manifest is unavailable or invalid",
            ) from exc
        if not isinstance(child, Phase1GDevExecutionManifest):
            raise Phase1GDevEvidenceError(
                REASON_REFERENCED_READBACK_FAILED,
                "G5 referenced manifest has the wrong contract type",
            )
        evidence.append({"kind": EvidenceKind.MANIFEST.value, "hash": identity})
        return child

    def close_rollback(
        rollback: Phase1GDevRollbackReceipt,
        inventory_ref: Phase1GDevEvidenceRef,
    ) -> None:
        rollback_manifest = load_manifest(rollback.input_manifest_hash)
        if (
            rollback_manifest.execution_mode is not ExecutionMode.ROLLBACK_VALIDATION
            or rollback_manifest.inventory_receipt_ref != inventory_ref
        ):
            raise Phase1GDevEvidenceError(
                REASON_REFERENCED_READBACK_FAILED,
                "rollback manifest does not close over the inventory",
            )

    def close_persistent(
        persistent: Phase1GDevPersistentReceipt,
        inventory_ref: Phase1GDevEvidenceRef,
    ) -> None:
        persistent_manifest = load_manifest(persistent.execution_manifest_hash)
        if (
            persistent.inventory_receipt_ref != inventory_ref
            or persistent_manifest.execution_mode
            is not ExecutionMode.PERSISTENT_DUAL_TRACK
            or persistent_manifest.inventory_receipt_ref != inventory_ref
        ):
            raise Phase1GDevEvidenceError(
                REASON_REFERENCED_READBACK_FAILED,
                "persistent evidence does not close over the inventory",
            )
        if persistent.batch_plan_ref is not None:
            plan = load_ref(persistent.batch_plan_ref, Phase1GExecutionBatchPlan)
            if str(plan.batch_plan_hash) != persistent.batch_plan_hash:
                raise Phase1GDevEvidenceError(
                    REASON_REFERENCED_READBACK_FAILED,
                    "persistent plan reference differs from receipt",
                )

    if isinstance(model, Phase1GDevExecutionManifest):
        load_ref(model.inventory_receipt_ref, Phase1GDevInputInventoryReceipt)
    elif isinstance(model, Phase1GDevRollbackReceipt):
        rollback_manifest = load_manifest(model.input_manifest_hash)
        load_ref(
            rollback_manifest.inventory_receipt_ref,
            Phase1GDevInputInventoryReceipt,
        )
        if rollback_manifest.execution_mode is not ExecutionMode.ROLLBACK_VALIDATION:
            raise Phase1GDevEvidenceError(
                REASON_REFERENCED_READBACK_FAILED,
                "rollback receipt references a non-rollback manifest",
            )
    elif isinstance(model, Phase1GDevPersistentReceipt):
        load_ref(model.inventory_receipt_ref, Phase1GDevInputInventoryReceipt)
        close_persistent(model, model.inventory_receipt_ref)
    elif isinstance(model, Phase1GDevEvidenceSummary):
        load_ref(model.inventory_receipt_ref, Phase1GDevInputInventoryReceipt)
        rollback = (
            load_ref(model.rollback_receipt_ref, Phase1GDevRollbackReceipt)
            if model.rollback_receipt_ref is not None
            else None
        )
        persistent = (
            load_ref(model.persistent_receipt_ref, Phase1GDevPersistentReceipt)
            if model.persistent_receipt_ref is not None
            else None
        )
        if rollback is not None:
            close_rollback(rollback, model.inventory_receipt_ref)
        if persistent is not None:
            close_persistent(persistent, model.inventory_receipt_ref)
        valid_status = (
            model.summary_status is SummaryStatus.COMPLETE
            and rollback is not None
            and rollback.rollback_status is RollbackStatus.COMPLETE_ZERO_RESIDUE
            and persistent is not None
            and persistent.persistent_status is PersistentStatus.COMPLETE_DUAL_TRACK
        ) or (
            model.summary_status is SummaryStatus.PENDING_L3_SOURCE
            and rollback is not None
            and rollback.rollback_status
            is RollbackStatus.NOT_RUN_SOURCE_EVIDENCE_PENDING
        ) or (
            model.summary_status is SummaryStatus.PENDING_L4
            and rollback is not None
            and rollback.rollback_status is RollbackStatus.COMPLETE_ZERO_RESIDUE
            and (
                persistent is None
                or persistent.persistent_status
                is PersistentStatus.NOT_RUN_INPUT_PENDING
            )
        ) or (
            model.summary_status is SummaryStatus.PARTIAL_FAILURE
            and persistent is not None
            and persistent.persistent_status is PersistentStatus.PARTIAL_FAILURE
        ) or (
            model.summary_status is SummaryStatus.FAILED
            and (
                (
                    rollback is not None
                    and rollback.rollback_status
                    in {RollbackStatus.FAILED, RollbackStatus.STATE_UNKNOWN}
                )
                or (
                    persistent is not None
                    and persistent.persistent_status is PersistentStatus.FAILED
                )
            )
        )
        if not valid_status:
            raise Phase1GDevEvidenceError(
                REASON_REFERENCED_READBACK_FAILED,
                "G5 summary status differs from referenced receipts",
            )
    return model, canonical_json_sha256(
        sorted(evidence, key=lambda item: (item["kind"], item["hash"]))
    )


class Phase1GDevEvidenceService:
    def __init__(
        self,
        *,
        env_file: Path,
        release_receipt_root: Path,
        phase1e_artifact_root: Path,
        phase1g_result_root: Path,
        evidence_store: Phase1GDevEvidenceStore,
        now_provider: NowProvider | None = None,
    ) -> None:
        self._env_file = env_file
        self._release_root = release_receipt_root
        self._phase1e_root = phase1e_artifact_root
        self._result_root = phase1g_result_root
        self._store = evidence_store
        self._now = now_provider or (lambda: datetime.now(timezone.utc))
        self._context = build_phase1g_command_context(
            env_file=env_file,
            target_label=TargetLabel.DEV,
            release_receipt_root=release_receipt_root,
            phase1e_artifact_root=phase1e_artifact_root,
            result_root=phase1g_result_root,
        )

    @property
    def context(self) -> Phase1GCommandContext:
        return self._context

    def inventory(self) -> StoredG5Evidence:
        receipt = Phase1GDevInventory(
            context=self._context,
            release_receipt_root=self._release_root,
            phase1e_artifact_root=self._phase1e_root,
            now_provider=self._now,
        ).run()
        return self._store.publish(receipt)

    def validate_rollback(
        self,
        *,
        inventory_ref: Phase1GDevEvidenceRef,
        manifest: Phase1GDevExecutionManifest,
    ) -> tuple[StoredG5Evidence, StoredG5Evidence]:
        inventory = self._load_inventory(inventory_ref)
        self._validate_manifest(
            inventory_ref=inventory_ref,
            inventory=inventory,
            manifest=manifest,
            expected_mode=ExecutionMode.ROLLBACK_VALIDATION,
        )
        self._store.publish(manifest)
        selected = {
            item.source_candidate_hash: item
            for item in inventory.l3_source_candidates
            if item.classification
            in {
                L3SourceClassification.ELIGIBLE_SINGLE,
                L3SourceClassification.ELIGIBLE_NATIVE_MULTI,
            }
        }
        candidates = tuple(selected.get(value) for value in manifest.source_candidate_hashes)
        if any(item is None for item in candidates):
            raise Phase1GDevEvidenceError(
                REASON_MANIFEST_INVALID,
                "rollback manifest selects a missing or ineligible L3 source",
            )
        if not candidates:
            receipt = self._pending_rollback(inventory=inventory, manifest=manifest)
            stored = self._store.publish(receipt)
            summary = self._store.publish(
                Phase1GDevEvidenceSummary(
                    inventory_receipt_ref=inventory_ref,
                    rollback_receipt_ref=stored.ref,
                    summary_status=SummaryStatus.PENDING_L3_SOURCE,
                    reason_codes=(REASON_L3_SOURCE_PENDING,),
                    created_at=self._aware_now(),
                )
            )
            return stored, summary
        return self._execute_rollback(
            inventory_ref=inventory_ref,
            inventory=inventory,
            manifest=manifest,
            candidates=tuple(item for item in candidates if item is not None),
        )

    def capture_persistent(
        self,
        *,
        inventory_ref: Phase1GDevEvidenceRef,
        rollback_ref: Phase1GDevEvidenceRef,
        manifest: Phase1GDevExecutionManifest,
    ) -> tuple[StoredG5Evidence, StoredG5Evidence]:
        inventory = self._load_inventory(inventory_ref)
        rollback = self._load_rollback(rollback_ref)
        rollback_manifest = self._store.load_by_identity(
            kind=EvidenceKind.MANIFEST,
            identity=rollback.input_manifest_hash,
        )
        if (
            rollback.rollback_status is not RollbackStatus.COMPLETE_ZERO_RESIDUE
            or rollback.catalog_fingerprint != inventory.catalog_fingerprint
            or rollback.database_identity != inventory.database_identity
            or not isinstance(rollback_manifest, Phase1GDevExecutionManifest)
            or rollback_manifest.execution_mode is not ExecutionMode.ROLLBACK_VALIDATION
            or rollback_manifest.inventory_receipt_ref != inventory_ref
        ):
            raise Phase1GDevEvidenceError(
                REASON_MANIFEST_INVALID,
                "persistent capture requires an exact COMPLETE_ZERO_RESIDUE receipt for the same inventory",
            )
        self._validate_manifest(
            inventory_ref=inventory_ref,
            inventory=inventory,
            manifest=manifest,
            expected_mode=ExecutionMode.PERSISTENT_DUAL_TRACK,
        )
        self._store.publish(manifest)
        executable = {
            str(item.target_request.request_hash): item
            for item in inventory.l4_target_candidates
            if item.executable and item.target_request is not None
        }
        candidates = tuple(executable.get(value) for value in manifest.target_request_hashes)
        if any(item is None for item in candidates):
            raise Phase1GDevEvidenceError(
                REASON_MANIFEST_INVALID,
                "persistent manifest selects a missing or non-executable target",
            )
        if inventory.inventory_status is not InventoryStatus.L4_DUAL_TRACK_READY:
            receipt = self._pending_persistent(
                inventory_ref=inventory_ref,
                inventory=inventory,
                manifest=manifest,
            )
            stored = self._store.publish(receipt)
            summary = self._store.publish(
                Phase1GDevEvidenceSummary(
                    inventory_receipt_ref=inventory_ref,
                    rollback_receipt_ref=rollback_ref,
                    persistent_receipt_ref=stored.ref,
                    summary_status=SummaryStatus.PENDING_L4,
                    reason_codes=(REASON_REAL_INPUT_PENDING,),
                    created_at=self._aware_now(),
                )
            )
            return stored, summary
        return self._execute_persistent(
            inventory_ref=inventory_ref,
            rollback_ref=rollback_ref,
            inventory=inventory,
            manifest=manifest,
            candidates=tuple(item for item in candidates if item is not None),
        )

    def verify_evidence(
        self,
        ref: Phase1GDevEvidenceRef,
        *,
        db_readback: bool = False,
    ) -> dict[str, Any]:
        model, reference_closure_hash = verify_g5_reference_closure(
            store=self._store,
            ref=ref,
        )
        readback_hash = None
        if db_readback:
            if not isinstance(model, Phase1GDevPersistentReceipt):
                raise Phase1GDevEvidenceError(
                    REASON_REFERENCED_READBACK_FAILED,
                    "DB referenced readback only applies to persistent receipts",
                )
            if len(model.batch_attempt_refs) != 2:
                raise Phase1GDevEvidenceError(
                    REASON_REFERENCED_READBACK_FAILED,
                    "persistent receipt does not carry first/rerun batch attempt refs",
                )
            phase_hashes: list[str] = []
            for index, batch_ref in enumerate(model.batch_attempt_refs):
                target_hashes: list[str] = []
                for target in model.target_outcomes:
                    attempt_ref = (
                        target.first_attempt_ref
                        if index == 0
                        else target.rerun_attempt_ref
                    )
                    if attempt_ref is None:
                        raise Phase1GDevEvidenceError(
                            REASON_REFERENCED_READBACK_FAILED,
                            "persistent target is missing first/rerun attempt ref",
                        )
                    attempt = self._context.result_store.load(attempt_ref)
                    if not isinstance(attempt, Phase1GAttemptReceipt):
                        raise Phase1GDevEvidenceError(
                            REASON_REFERENCED_READBACK_FAILED,
                            "persistent target attempt ref has the wrong type",
                        )
                    target_hashes.append(
                        verify_phase1g_attempt_database(
                            receipt=attempt,
                            result_store=self._context.result_store,
                            connection_config=self._context.connection_config,
                        ).evidence_hash
                    )
                batch = self._context.result_store.load(batch_ref)
                if not isinstance(batch, Phase1GBatchAttemptReceipt):
                    raise Phase1GDevEvidenceError(
                        REASON_REFERENCED_READBACK_FAILED,
                        "persistent batch attempt ref has the wrong type",
                    )
                target_hashes.append(
                    verify_phase1g_attempt_database(
                        receipt=batch,
                        result_store=self._context.result_store,
                        connection_config=self._context.connection_config,
                    ).evidence_hash
                )
                phase_hashes.append(canonical_json_sha256(sorted(target_hashes)))
            readback_hash = canonical_json_sha256(
                {"first": phase_hashes[0], "rerun": phase_hashes[1]}
            )
            if readback_hash != model.referenced_readback_hash:
                raise Phase1GDevEvidenceError(
                    REASON_REFERENCED_READBACK_FAILED,
                    "persistent referenced readback hash differs from receipt",
                )
        return {
            "ok": True,
            "evidence_kind": ref.evidence_kind.value,
            "semantic_content_hash": ref.semantic_content_hash,
            "model_schema_version": getattr(model, "schema_version", None),
            "db_readback": db_readback,
            "referenced_readback_hash": readback_hash,
            "reference_closure_hash": reference_closure_hash,
        }

    def _execute_rollback(
        self,
        *,
        inventory_ref: Phase1GDevEvidenceRef,
        inventory: Phase1GDevInputInventoryReceipt,
        manifest: Phase1GDevExecutionManifest,
        candidates,  # type: ignore[no-untyped-def]
    ) -> tuple[StoredG5Evidence, StoredG5Evidence]:
        started_at = self._aware_now()
        invocation_id = f"p1g_g5_l3_{uuid4().hex}"
        temp_root = Path(
            tempfile.mkdtemp(
                prefix=f".{invocation_id}.",
                dir=self._store.root.parent,
            )
        ).resolve(strict=True)
        ephemeral_phase1e = temp_root / "phase1e"
        ephemeral_results = temp_root / "results"
        ephemeral_phase1e.mkdir()
        ephemeral_results.mkdir()
        composer = Phase1GL3ValidationEvidenceComposer(
            source_resolver=self._context.artifact_resolver
        )
        coordinator: Phase1GDevRollbackCoordinator | None = None
        batch_plan_hash: str | None = None
        outcome_hash: str | None = None
        ephemeral_hashes: tuple[str, ...] = ()
        residue_checks = ()
        concurrency_hash: str | None = None
        reasons: set[str] = set()
        status = RollbackStatus.FAILED
        disposed = False
        control_request: ControlBindingRequest | None = None
        try:
            requests = tuple(
                composer.compose(
                    candidate=candidate,
                    ephemeral_phase1e_root=ephemeral_phase1e,
                    requested_at=started_at,
                )[0]
                for candidate in candidates
            )
            preload_context = build_phase1g_command_context(
                env_file=self._env_file,
                target_label=TargetLabel.DEV,
                release_receipt_root=self._release_root,
                phase1e_artifact_root=ephemeral_phase1e,
                result_root=ephemeral_results,
            )
            preloaded_targets = {
                str(request.request_hash): preload_context.service._load_target(request)
                for request in requests
            }
            if len(preloaded_targets) != len(requests):
                raise Phase1GDevEvidenceError(
                    REASON_MANIFEST_INVALID,
                    "rollback immutable preload did not preserve every target",
                )
            coordinator = Phase1GDevRollbackCoordinator(
                connection_factory=self._connect,
                application_name=f"aistock:g5:l3:{invocation_id[-16:]}",
                statement_timeout_ms=DEFAULT_CAPTURE_POLICY_REGISTRY.statement_timeout_ms,
                lock_timeout_ms=DEFAULT_CAPTURE_POLICY_REGISTRY.lock_timeout_ms,
            )
            probes = ()
            with coordinator:
                rollback_context = build_phase1g_command_context(
                    env_file=self._env_file,
                    target_label=TargetLabel.DEV,
                    release_receipt_root=self._release_root,
                    phase1e_artifact_root=ephemeral_phase1e,
                    result_root=ephemeral_results,
                    transaction_connection_factory=coordinator.transaction_connection_factory,
                    readonly_connection_factory=coordinator.readonly_connection_factory,
                )
                rollback_service = _PreloadedRollbackPhase1GService(
                    preloaded_targets=preloaded_targets,
                    connection_config=rollback_context.connection_config,
                    transaction_connection_factory=(
                        coordinator.transaction_connection_factory
                    ),
                    readonly_connection_factory=coordinator.readonly_connection_factory,
                    artifact_resolver=rollback_context.artifact_resolver,
                    result_store=rollback_context.result_store,
                    now_provider=self._now,
                )
                rollback_context = Phase1GCommandContext(
                    connection_config=rollback_context.connection_config,
                    artifact_resolver=rollback_context.artifact_resolver,
                    result_store=rollback_context.result_store,
                    service=rollback_service,
                )
                plan = rollback_context.service.plan_batch(
                    Phase1GExecutionBatchRequest(targets=requests)
                )
                batch_plan_hash = str(plan.batch_plan_hash)
                outcome = rollback_context.service.capture_batch(plan)
                outcome_hash = canonical_json_sha256(outcome.model_dump(mode="json"))
                ephemeral_hashes = tuple(
                    sorted(
                        {
                            value
                            for item in outcome.target_outcomes
                            for value in (
                                item.capture_result_hash,
                                item.attempt_receipt_hash,
                            )
                            if value is not None
                        }
                        | (
                            {outcome.batch_attempt_receipt_hash}
                            if outcome.batch_attempt_receipt_hash is not None
                            else set()
                        )
                    )
                )
                if outcome.batch_status is not Phase1GInvocationBatchStatus.SUCCESS:
                    reasons.update(outcome.reason_codes or {REASON_L3_ROLLBACK_FAILED})
                first_result = next(
                    (
                        rollback_context.result_store.load(item.capture_result_ref)
                        for item in outcome.target_outcomes
                        if item.capture_result_ref is not None
                    ),
                    None,
                )
                if first_result is not None:
                    with coordinator.owner_cursor(
                        cursor_factory=psycopg2.extras.RealDictCursor
                    ) as cur:
                        event = PostgresControlBindingRepository.read_exact_in_transaction(
                            cur, first_result.control_binding_event_hash
                        )
                        control_request = event.request
                        probes = capture_current_transaction_residue_probes(cursor=cur)
            residue_checks = verify_zero_residue(
                connection_factory=self._connect,
                probes=probes,
            )
            if control_request is None:
                reasons.add(REASON_L3_ROLLBACK_FAILED)
            else:
                concurrency_hash = run_control_binding_concurrency_probe(
                    connection_factory=self._connect,
                    request=self._fresh_probe_request(control_request),
                    lock_timeout_ms=DEFAULT_CAPTURE_POLICY_REGISTRY.lock_timeout_ms,
                )
            summary = coordinator.recorder.summary()
            if (
                not reasons
                and summary.observed_transactional_dml
                and coordinator.physical_rollback_count == 1
                and concurrency_hash is not None
            ):
                status = RollbackStatus.COMPLETE_ZERO_RESIDUE
            else:
                reasons.add(REASON_L3_ROLLBACK_FAILED)
        except Exception as exc:  # Every failure becomes durable evidence; unexpected keeps a traceback.
            reason = str(getattr(exc, "reason_code", REASON_UNEXPECTED_ERROR))
            reasons.add(reason)
            if reason == REASON_UNEXPECTED_ERROR:
                _log_sanitized_exception(
                    "phase1g G5 L3 unexpected failure invocation_id=%s",
                    exc,
                    invocation_id,
                )
        finally:
            try:
                _remove_ephemeral_root(temp_root=temp_root, allowed_parent=self._store.root.parent)
                disposed = True
            except Exception as exc:
                _log_sanitized_exception(
                    "phase1g G5 L3 ephemeral cleanup failed invocation_id=%s",
                    exc,
                    invocation_id,
                )
                reasons.add(REASON_EVIDENCE_STORE_FAILED)
                status = RollbackStatus.FAILED
        query_summary = (
            coordinator.recorder.summary()
            if coordinator is not None
            else _empty_query_summary()
        )
        receipt = Phase1GDevRollbackReceipt(
            rollback_invocation_id=invocation_id,
            database_identity=inventory.database_identity,
            catalog_fingerprint=inventory.catalog_fingerprint,
            input_manifest_hash=str(manifest.manifest_hash),
            batch_plan_hash=batch_plan_hash,
            observed_transactional_dml=query_summary.observed_transactional_dml,
            physical_rollback_count=(
                coordinator.physical_rollback_count if coordinator is not None else 0
            ),
            read_query_count=query_summary.read_query_count,
            write_query_count=query_summary.write_query_count,
            normalized_query_set_hash=query_summary.normalized_query_set_hash,
            write_relation_set=query_summary.write_relation_set,
            in_transaction_outcome_hash=outcome_hash,
            ephemeral_result_hashes=ephemeral_hashes,
            ephemeral_artifacts_disposed=disposed,
            fresh_connection_residue_checks=residue_checks,
            concurrency_probe_hash=concurrency_hash,
            rollback_status=status,
            reason_codes=tuple(sorted(reasons)),
            started_at=started_at,
            finished_at=self._aware_now(),
        )
        stored = self._store.publish(receipt)
        summary_status = (
            SummaryStatus.PENDING_L4
            if status is RollbackStatus.COMPLETE_ZERO_RESIDUE
            else SummaryStatus.FAILED
        )
        summary = self._store.publish(
            Phase1GDevEvidenceSummary(
                inventory_receipt_ref=inventory_ref,
                rollback_receipt_ref=stored.ref,
                summary_status=summary_status,
                reason_codes=(
                    (REASON_REAL_INPUT_PENDING,)
                    if summary_status is SummaryStatus.PENDING_L4
                    else tuple(sorted(reasons))
                ),
                created_at=self._aware_now(),
            )
        )
        return stored, summary

    def _execute_persistent(
        self,
        *,
        inventory_ref: Phase1GDevEvidenceRef,
        rollback_ref: Phase1GDevEvidenceRef,
        inventory: Phase1GDevInputInventoryReceipt,
        manifest: Phase1GDevExecutionManifest,
        candidates,  # type: ignore[no-untyped-def]
    ) -> tuple[StoredG5Evidence, StoredG5Evidence]:
        started_at = self._aware_now()
        invocation_id = f"p1g_g5_l4_{uuid4().hex}"
        reasons: set[str] = set()
        plan_ref = None
        plan_hash = None
        first_hash = None
        rerun_hash = None
        readback_hash = None
        first = None
        rerun = None
        first_readback = None
        rerun_readback = None
        try:
            requests = tuple(item.target_request for item in candidates)
            if any(item is None for item in requests):
                raise Phase1GDevEvidenceError(
                    REASON_MANIFEST_INVALID,
                    "persistent candidates must carry target requests",
                )
            plan = self._context.service.plan_batch(
                Phase1GExecutionBatchRequest(
                    targets=tuple(item for item in requests if item is not None)
                )
            )
            stored_plan = self._store.publish(plan)
            plan_ref = stored_plan.ref
            plan_hash = str(plan.batch_plan_hash)
            loaded_plan = self._store.load(stored_plan.ref)
            if loaded_plan != plan:
                raise Phase1GDevEvidenceError(
                    REASON_EVIDENCE_STORE_FAILED,
                    "persistent plan readback differs",
                )
            first = self._context.service.capture_batch(plan)
            first_hash = canonical_json_sha256(first.model_dump(mode="json"))
            first_readback = self._verify_batch_outcome(first)
            rerun = self._context.service.capture_batch(plan)
            rerun_hash = canonical_json_sha256(rerun.model_dump(mode="json"))
            rerun_readback = self._verify_batch_outcome(rerun)
            readback_hash = canonical_json_sha256(
                {
                    "first": first_readback,
                    "rerun": rerun_readback,
                }
            )
        except Exception as exc:
            reason = str(getattr(exc, "reason_code", REASON_UNEXPECTED_ERROR))
            reasons.update(_persistent_reason_set((reason,)))
            if reason == REASON_UNEXPECTED_ERROR:
                _log_sanitized_exception(
                    "phase1g G5 L4 unexpected failure invocation_id=%s",
                    exc,
                    invocation_id,
                )
        target_receipts = _build_persistent_target_outcomes(
            candidates=candidates,
            first=first,
            rerun=rerun,
            global_reasons=reasons,
        )
        for target in target_receipts:
            if not target.exact_rerun_verified:
                reasons.update(target.reason_codes)
        batch_refs = tuple(
            ref
            for ref in (
                first.batch_attempt_receipt_ref if first is not None else None,
                rerun.batch_attempt_receipt_ref if rerun is not None else None,
            )
            if ref is not None
        )
        single_count = sum(item.alpha_mode is AlphaMode.SINGLE for item in candidates)
        multi_count = sum(item.alpha_mode is AlphaMode.MULTI for item in candidates)
        first_dml = (
            sum(item.dml_executed for item in first.target_outcomes)
            if first is not None
            else 0
        )
        rerun_dml = (
            sum(item.dml_executed for item in rerun.target_outcomes)
            if rerun is not None
            else 0
        )
        complete = (
            not reasons
            and first is not None
            and rerun is not None
            and first.batch_status is Phase1GInvocationBatchStatus.SUCCESS
            and rerun.batch_status is Phase1GInvocationBatchStatus.SUCCESS
            and rerun_dml == 0
            and len(target_receipts) == len(candidates)
            and all(item.exact_rerun_verified for item in target_receipts)
            and len(batch_refs) == 2
            and batch_refs[0].semantic_content_hash
            != batch_refs[1].semantic_content_hash
            and first_hash != rerun_hash
            and first_readback is not None
            and rerun_readback is not None
        )
        status = (
            PersistentStatus.COMPLETE_DUAL_TRACK
            if complete
            else PersistentStatus.PARTIAL_FAILURE
            if first is not None or rerun is not None
            else PersistentStatus.FAILED
        )
        if status is not PersistentStatus.COMPLETE_DUAL_TRACK:
            reasons.add(REASON_L4_PARTIAL_FAILURE)
        stable_set_hash = (
            canonical_json_sha256(
                sorted(
                    str(item.stable_result_hash)
                    for item in target_receipts
                    if item.stable_result_hash is not None
                )
            )
            if any(item.stable_result_hash is not None for item in target_receipts)
            else None
        )
        receipt = Phase1GDevPersistentReceipt(
            persistent_invocation_id=invocation_id,
            database_identity=inventory.database_identity,
            catalog_fingerprint=inventory.catalog_fingerprint,
            inventory_receipt_ref=inventory_ref,
            execution_manifest_hash=str(manifest.manifest_hash),
            batch_plan_ref=plan_ref,
            batch_plan_hash=plan_hash,
            first_batch_outcome_hash=first_hash,
            rerun_batch_outcome_hash=rerun_hash,
            target_outcomes=tuple(target_receipts),
            batch_attempt_refs=batch_refs,
            single_target_count=single_count,
            native_multi_target_count=multi_count,
            first_dml_target_count=first_dml,
            rerun_dml_target_count=rerun_dml,
            stable_result_set_hash=stable_set_hash,
            referenced_readback_hash=readback_hash,
            persistent_status=status,
            reason_codes=tuple(sorted(reasons)),
            started_at=started_at,
            finished_at=self._aware_now(),
        )
        stored = self._store.publish(receipt)
        summary = self._store.publish(
            Phase1GDevEvidenceSummary(
                inventory_receipt_ref=inventory_ref,
                rollback_receipt_ref=rollback_ref,
                persistent_receipt_ref=stored.ref,
                summary_status=_persistent_summary_status(status),
                reason_codes=tuple(sorted(reasons)),
                created_at=self._aware_now(),
            )
        )
        return stored, summary

    def _verify_batch_outcome(self, outcome) -> str:  # type: ignore[no-untyped-def]
        evidence: list[str] = []
        for target in outcome.target_outcomes:
            if target.attempt_receipt_ref is None:
                raise Phase1GDevEvidenceError(
                    REASON_REFERENCED_READBACK_FAILED,
                    "target outcome is missing its durable attempt ref",
                )
            receipt = self._context.result_store.load(target.attempt_receipt_ref)
            if not isinstance(receipt, Phase1GAttemptReceipt):
                raise Phase1GDevEvidenceError(
                    REASON_REFERENCED_READBACK_FAILED,
                    "target attempt ref resolves to the wrong type",
                )
            if (
                target.attempt_receipt_hash != receipt.attempt_receipt_hash
                or target.attempt_receipt_ref.semantic_content_hash
                != receipt.attempt_receipt_hash
                or target.target_request_hash != receipt.target_request_hash
                or target.target_plan_hash != receipt.target_plan_hash
                or target.operation_status.value != receipt.operation_status.value
                or target.dml_executed != receipt.dml_executed
                or tuple(target.committed_phases)
                != tuple(receipt.committed_phases)
                or target.capture_result_hash != receipt.capture_result_hash
            ):
                raise Phase1GDevEvidenceError(
                    REASON_REFERENCED_READBACK_FAILED,
                    "target outcome differs from its durable attempt receipt",
                )
            evidence.append(
                verify_phase1g_attempt_database(
                    receipt=receipt,
                    result_store=self._context.result_store,
                    connection_config=self._context.connection_config,
                ).evidence_hash
            )
        if outcome.batch_attempt_receipt_ref is None:
            raise Phase1GDevEvidenceError(
                REASON_REFERENCED_READBACK_FAILED,
                "batch outcome is missing its durable batch attempt ref",
            )
        batch = self._context.result_store.load(outcome.batch_attempt_receipt_ref)
        if not isinstance(batch, Phase1GBatchAttemptReceipt):
            raise Phase1GDevEvidenceError(
                REASON_REFERENCED_READBACK_FAILED,
                "batch attempt ref resolves to the wrong type",
            )
        if (
            outcome.batch_attempt_receipt_hash
            != batch.batch_attempt_receipt_hash
            or outcome.batch_attempt_receipt_ref.semantic_content_hash
            != batch.batch_attempt_receipt_hash
            or outcome.batch_request_hash != batch.batch_request_hash
            or outcome.batch_plan_hash != batch.batch_plan_hash
            or outcome.succeeded_count != batch.succeeded_count
            or outcome.failed_count != batch.failed_count
            or outcome.batch_status.value != batch.batch_status.value
        ):
            raise Phase1GDevEvidenceError(
                REASON_REFERENCED_READBACK_FAILED,
                "batch outcome differs from its durable batch attempt receipt",
            )
        evidence.append(
            verify_phase1g_attempt_database(
                receipt=batch,
                result_store=self._context.result_store,
                connection_config=self._context.connection_config,
            ).evidence_hash
        )
        return canonical_json_sha256(sorted(evidence))

    def _load_inventory(
        self, ref: Phase1GDevEvidenceRef
    ) -> Phase1GDevInputInventoryReceipt:
        if ref.evidence_kind is not EvidenceKind.INVENTORY:
            raise Phase1GDevEvidenceError(
                REASON_MANIFEST_INVALID,
                "inventory ref has the wrong evidence kind",
            )
        model = self._store.load(ref)
        if not isinstance(model, Phase1GDevInputInventoryReceipt):
            raise Phase1GDevEvidenceError(
                REASON_MANIFEST_INVALID,
                "inventory ref resolves to the wrong model",
            )
        return model

    def _load_rollback(
        self, ref: Phase1GDevEvidenceRef
    ) -> Phase1GDevRollbackReceipt:
        if ref.evidence_kind is not EvidenceKind.ROLLBACK:
            raise Phase1GDevEvidenceError(
                REASON_MANIFEST_INVALID,
                "rollback ref has the wrong evidence kind",
            )
        model = self._store.load(ref)
        if not isinstance(model, Phase1GDevRollbackReceipt):
            raise Phase1GDevEvidenceError(
                REASON_MANIFEST_INVALID,
                "rollback ref resolves to the wrong model",
            )
        return model

    @staticmethod
    def _validate_manifest(
        *,
        inventory_ref: Phase1GDevEvidenceRef,
        inventory: Phase1GDevInputInventoryReceipt,
        manifest: Phase1GDevExecutionManifest,
        expected_mode: ExecutionMode,
    ) -> None:
        if (
            manifest.execution_mode is not expected_mode
            or manifest.inventory_receipt_ref != inventory_ref
            or manifest.inventory_receipt_ref.semantic_content_hash
            != inventory.inventory_receipt_hash
        ):
            raise Phase1GDevEvidenceError(
                REASON_MANIFEST_INVALID,
                "execution manifest does not match requested inventory or mode",
            )
        if (
            expected_mode is ExecutionMode.ROLLBACK_VALIDATION
            and inventory.l3_source_eligible_count > 0
            and not manifest.source_candidate_hashes
        ):
            raise Phase1GDevEvidenceError(
                REASON_MANIFEST_INVALID,
                "rollback manifest must select an eligible source when inventory provides one",
            )
        if expected_mode is ExecutionMode.PERSISTENT_DUAL_TRACK:
            if (
                inventory.inventory_status is InventoryStatus.L4_DUAL_TRACK_READY
                and not manifest.target_request_hashes
            ):
                raise Phase1GDevEvidenceError(
                    REASON_MANIFEST_INVALID,
                    "persistent manifest must select dual-track targets when inventory is ready",
                )
            by_hash = {
                str(item.target_request.request_hash): item
                for item in inventory.l4_target_candidates
                if item.executable and item.target_request is not None
            }
            selected = [by_hash.get(value) for value in manifest.target_request_hashes]
            if any(item is None for item in selected):
                raise Phase1GDevEvidenceError(
                    REASON_MANIFEST_INVALID,
                    "manifest target is absent from executable inventory",
                )
            single = sum(item.alpha_mode is AlphaMode.SINGLE for item in selected if item is not None)
            multi = sum(item.alpha_mode is AlphaMode.MULTI for item in selected if item is not None)
            if single != manifest.single_target_count or multi != manifest.native_multi_target_count:
                raise Phase1GDevEvidenceError(
                    REASON_MANIFEST_INVALID,
                    "manifest alpha counts differ from inventory targets",
                )

    def _pending_rollback(
        self,
        *,
        inventory: Phase1GDevInputInventoryReceipt,
        manifest: Phase1GDevExecutionManifest,
    ) -> Phase1GDevRollbackReceipt:
        now = self._aware_now()
        return Phase1GDevRollbackReceipt(
            rollback_invocation_id=f"p1g_g5_l3_{uuid4().hex}",
            database_identity=inventory.database_identity,
            catalog_fingerprint=inventory.catalog_fingerprint,
            input_manifest_hash=str(manifest.manifest_hash),
            observed_transactional_dml=False,
            physical_rollback_count=0,
            read_query_count=0,
            write_query_count=0,
            normalized_query_set_hash=canonical_json_sha256([]),
            ephemeral_artifacts_disposed=True,
            rollback_status=RollbackStatus.NOT_RUN_SOURCE_EVIDENCE_PENDING,
            reason_codes=(REASON_L3_SOURCE_PENDING,),
            started_at=now,
            finished_at=now,
        )

    def _pending_persistent(
        self,
        *,
        inventory_ref: Phase1GDevEvidenceRef,
        inventory: Phase1GDevInputInventoryReceipt,
        manifest: Phase1GDevExecutionManifest,
    ) -> Phase1GDevPersistentReceipt:
        now = self._aware_now()
        return Phase1GDevPersistentReceipt(
            persistent_invocation_id=f"p1g_g5_l4_{uuid4().hex}",
            database_identity=inventory.database_identity,
            catalog_fingerprint=inventory.catalog_fingerprint,
            inventory_receipt_ref=inventory_ref,
            execution_manifest_hash=str(manifest.manifest_hash),
            single_target_count=0,
            native_multi_target_count=0,
            first_dml_target_count=0,
            rerun_dml_target_count=0,
            persistent_status=PersistentStatus.NOT_RUN_INPUT_PENDING,
            reason_codes=(REASON_REAL_INPUT_PENDING,),
            started_at=now,
            finished_at=now,
        )

    def _connect(self) -> Any:
        connection = psycopg2.connect(**self._context.connection_config.connect_kwargs())
        connection.autocommit = False
        return connection

    def _fresh_probe_request(
        self, captured_request: ControlBindingRequest
    ) -> ControlBindingRequest:
        connection = self._connect()
        try:
            connection.set_session(
                readonly=True,
                autocommit=False,
                isolation_level="REPEATABLE READ",
            )
            with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                try:
                    head = PostgresControlBindingRepository.current_readonly(
                        cur, captured_request.binding_chain_key
                    )
                except SourceLedgerError as exc:
                    if exc.reason_code != REASON_CONTROL_BINDING_UNAVAILABLE:
                        raise
                    head = None
            connection.rollback()
        finally:
            connection.close()
        if head is None:
            payload = captured_request.model_dump(mode="python")
            payload.update(
                binding_event_revision_no=1,
                predecessor_binding_event_hash=None,
                created_by_service_principal="advisory_phase1g_g5_concurrency_probe",
            )
            return ControlBindingRequest.model_validate(payload)
        config = {
            **head.request.config_payload,
            "g5_concurrency_probe_nonce": uuid4().hex,
        }
        payload = head.request.model_dump(mode="python")
        payload.update(
            config_payload=config,
            config_or_store_backend_hash=canonical_json_sha256(config),
            binding_event_revision_no=head.request.binding_event_revision_no + 1,
            predecessor_binding_event_hash=head.binding_event_hash,
            created_by_service_principal="advisory_phase1g_g5_concurrency_probe",
        )
        return ControlBindingRequest.model_validate(payload)

    def _aware_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise Phase1GDevEvidenceError(
                REASON_UNEXPECTED_ERROR,
                "G5 clock must be timezone-aware",
            )
        return value.astimezone(timezone.utc)


def _remove_ephemeral_root(*, temp_root: Path, allowed_parent: Path) -> None:
    resolved = temp_root.resolve(strict=True)
    parent = allowed_parent.resolve(strict=True)
    if resolved.parent != parent or not resolved.name.startswith(".p1g_g5_l3_"):
        raise Phase1GDevEvidenceError(
            REASON_EVIDENCE_STORE_FAILED,
            "refusing to remove an unrecognized L3 temporary root",
        )
    shutil.rmtree(resolved)


def _empty_query_summary():  # type: ignore[no-untyped-def]
    from .phase1g_dev_rollback import Phase1GDevQuerySummary

    return Phase1GDevQuerySummary(
        evidence=(),
        read_query_count=0,
        write_query_count=0,
        normalized_query_set_hash=canonical_json_sha256([]),
        write_relation_set=(),
        observed_transactional_dml=False,
    )
