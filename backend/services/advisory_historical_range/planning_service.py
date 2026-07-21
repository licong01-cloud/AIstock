"""Create, resume, verify, and seal Phase 1R source-catalog planning."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .artifact_store import HistoricalRangeArtifactStore
from .calendar_resolver import HistoricalRangeCalendarResolver
from .catalog_planner import REASON_SOURCE_REVISION_DRIFT
from .catalog_postgres import PostgresHistoricalRangeCatalogExecutor
from .code_release import HistoricalRangeCodeReleaseResolver
from .models import (
    SOURCE_CATALOG_CHECKPOINT_SCHEMA_VERSION,
    HMM_BINDING_SET_SCHEMA_VERSION,
    HistoricalRangeArtifactBindingsV1,
    HistoricalRangeArtifactKind,
    HistoricalRangeCatalogPhase,
    HistoricalRangeContractError,
    HistoricalRangeHMMBindingSetV1,
    HistoricalRangeHMMBindingV1,
    HistoricalRangeOperationStatus,
    HistoricalRangePlanningArtifactBindingsV1,
    HistoricalRangeResolvedRequestArtifactPayloadV1,
    HistoricalRangeResearchBatchRequestV1,
    HistoricalRangeSourceCatalogCheckpointV1,
    HistoricalRangeSourceRevisionCatalogV1,
    HistoricalRangeUnresolvedRequirementV1,
    ResolvedHistoricalRangeRequestV1,
)
from .repository import (
    CreatedHistoricalRangePlanningBatch,
    PostgresHistoricalRangeRepository,
    SealedHistoricalRangeBatch,
)
from .request_resolver import HistoricalRangeProgramResolver
from .requirement_planner import HistoricalRangeSourceRequirementPlanner


PLANNING_PRODUCER_CONTRACT_VERSION = "phase1r_r2b"


@dataclass(frozen=True)
class HistoricalRangeCatalogChunkExecutionResult:
    operation: dict[str, Any]
    sealed_batch: SealedHistoricalRangeBatch | None = None


class HistoricalRangePlanningService:
    def __init__(
        self,
        *,
        program_resolver: HistoricalRangeProgramResolver,
        calendar_resolver: HistoricalRangeCalendarResolver,
        code_release_resolver: HistoricalRangeCodeReleaseResolver,
        requirement_planner: HistoricalRangeSourceRequirementPlanner,
        catalog_executor: PostgresHistoricalRangeCatalogExecutor,
        repository: PostgresHistoricalRangeRepository,
        artifact_store: HistoricalRangeArtifactStore,
        selection_semantics_version: str,
        selection_semantics_hash: str,
        list_semantics_version: str,
        list_semantics_hash: str,
    ) -> None:
        dependencies = (
            program_resolver,
            calendar_resolver,
            code_release_resolver,
            requirement_planner,
            catalog_executor,
            repository,
            artifact_store,
        )
        if any(item is None for item in dependencies):
            raise ValueError("historical planning service requires explicit dependencies")
        self._program_resolver = program_resolver
        self._calendar_resolver = calendar_resolver
        self._code_release_resolver = code_release_resolver
        self._requirement_planner = requirement_planner
        self._catalog_executor = catalog_executor
        self._repository = repository
        self._artifact_store = artifact_store
        self._selection_semantics_version = selection_semantics_version
        self._selection_semantics_hash = selection_semantics_hash
        self._list_semantics_version = list_semantics_version
        self._list_semantics_hash = list_semantics_hash

    def create(self, request: HistoricalRangeResearchBatchRequestV1) -> CreatedHistoricalRangePlanningBatch:
        release = self._code_release_resolver.resolve()
        programs = self._program_resolver.freeze_programs(
            request=request,
            code_release_id=release.code_release_id,
            code_release_hash=release.code_release_hash,
            selection_semantics_version=self._selection_semantics_version,
            selection_semantics_hash=self._selection_semantics_hash,
            list_semantics_version=self._list_semantics_version,
            list_semantics_hash=self._list_semantics_hash,
        )
        date_plan, calendar_identity_hash = self._calendar_resolver.resolve(
            request=request,
            frozen_programs=programs,
        )
        plan = self._requirement_planner.build(
            request=request,
            date_plan=date_plan,
            frozen_programs=programs,
            calendar_identity_hash=calendar_identity_hash,
            code_release_hash=release.code_release_hash,
            code_release_manifest=release.semantic_payload(),
        )
        stored = self._artifact_store.publish_planning_payload(
            artifact_kind=HistoricalRangeArtifactKind.SOURCE_REQUIREMENT_PLAN,
            planning_identity_hash=plan.planning_identity_hash,
            batch_id=plan.batch_id,
            catalog_generation=1,
            producer_contract_version=PLANNING_PRODUCER_CONTRACT_VERSION,
            payload_schema_version=plan.schema_version,
            payload=plan.model_dump(mode="json"),
        )
        return self._repository.create_planning_batch(
            plan=plan,
            artifacts=HistoricalRangePlanningArtifactBindingsV1(
                requirement_plan_ref=stored.ref,
                artifact_root_identity_hash=self._artifact_store.root_identity_hash,
            ),
        )

    def execute_claimed_chunk(
        self,
        *,
        operation_id: str,
        expected_row_version: int,
        expected_fencing_token: int,
        next_worker_id: str,
        next_lease_token: str,
        next_lease_expires_at: datetime,
        chunk_size: int = 32,
    ) -> HistoricalRangeCatalogChunkExecutionResult:
        state = self._repository.load_catalog_planning_state(operation_id=operation_id)
        operation = state.operation
        if (
            str(operation["status"]) != HistoricalRangeOperationStatus.RUNNING.value
            or int(operation["row_version"]) != expected_row_version
            or int(operation["fencing_token"]) != expected_fencing_token
        ):
            raise HistoricalRangeContractError(
                "ADVISORY_HR_CATALOG_CLAIM_CONFLICT",
                "catalog worker claim differs from durable operation state",
                context={"operation_id": operation_id},
            )
        phase = HistoricalRangeCatalogPhase(str(operation["catalog_phase"]))
        cursor = int((operation.get("stable_keyset_cursor_json") or {}).get("next_requirement_ordinal") or 1)
        previous_ref, previous_checkpoint = (
            state.checkpoint_chain[-1] if state.checkpoint_chain else (None, None)
        )
        try:
            chunk = self._catalog_executor.resolve_chunk(
                plan=state.plan,
                catalog_generation=int(operation["catalog_generation"]),
                phase=phase,
                start_ordinal=cursor,
                resolved_members=state.current_phase_members,
                expected_members=(state.discovered_members if phase is HistoricalRangeCatalogPhase.VERIFY else None),
                previous_checkpoint_ref=previous_ref,
                previous_checkpoint=previous_checkpoint,
                chunk_size=chunk_size,
            )
        except HistoricalRangeContractError as exc:
            if exc.reason_code != REASON_SOURCE_REVISION_DRIFT:
                raise
            drift_checkpoint = self._drift_checkpoint(state=state, ordinal=cursor, error=exc)
            drift_ref = self._publish_checkpoint(state=state, checkpoint=drift_checkpoint)
            restarted = self._repository.restart_catalog_generation(
                operation_id=operation_id,
                expected_row_version=expected_row_version,
                expected_fencing_token=expected_fencing_token,
                drift_receipt_ref=drift_ref,
                next_worker_id=next_worker_id,
                next_lease_token=next_lease_token,
                next_lease_expires_at=next_lease_expires_at,
                error_json={"reason_code": exc.reason_code, "message": str(exc), "context": exc.context},
            )
            return HistoricalRangeCatalogChunkExecutionResult(operation=restarted)
        checkpoint_ref = self._publish_checkpoint(state=state, checkpoint=chunk.checkpoint)
        if chunk.waiting_input:
            reasons = tuple(item.reason_code for item in chunk.checkpoint.unresolved_requirement_delta)
            updated = self._repository.commit_catalog_checkpoint(
                operation_id=operation_id,
                expected_row_version=expected_row_version,
                expected_fencing_token=expected_fencing_token,
                checkpoint_ref=checkpoint_ref,
                checkpoint=chunk.checkpoint,
                target_status=HistoricalRangeOperationStatus.WAITING_INPUT,
                reason_codes=reasons,
                error_json={
                    "reason_codes": list(reasons),
                    "unresolved": [
                        item.model_dump(mode="json") for item in chunk.checkpoint.unresolved_requirement_delta
                    ],
                },
            )
            return HistoricalRangeCatalogChunkExecutionResult(operation=updated)
        if chunk.phase_complete and phase is HistoricalRangeCatalogPhase.VERIFY:
            updated = self._repository.commit_catalog_checkpoint(
                operation_id=operation_id,
                expected_row_version=expected_row_version,
                expected_fencing_token=expected_fencing_token,
                checkpoint_ref=checkpoint_ref,
                checkpoint=chunk.checkpoint,
                target_status=HistoricalRangeOperationStatus.COMPLETED,
            )
            sealed = self._seal(operation_id=operation_id)
            return HistoricalRangeCatalogChunkExecutionResult(operation=updated, sealed_batch=sealed)
        updated = self._repository.commit_catalog_checkpoint(
            operation_id=operation_id,
            expected_row_version=expected_row_version,
            expected_fencing_token=expected_fencing_token,
            checkpoint_ref=checkpoint_ref,
            checkpoint=chunk.checkpoint,
            target_status=HistoricalRangeOperationStatus.RUNNING,
            advance_to_verify=chunk.phase_complete,
            next_worker_id=next_worker_id,
            next_lease_token=next_lease_token,
            next_lease_expires_at=next_lease_expires_at,
        )
        return HistoricalRangeCatalogChunkExecutionResult(operation=updated)

    def seal_completed_catalog(self, *, operation_id: str) -> SealedHistoricalRangeBatch:
        """Retry the idempotent request seal after a completed VERIFY operation.

        The final checkpoint and operation completion are durable before external
        CAS/request sealing. A process failure between those steps must not make
        the planning batch unrecoverable.
        """

        state = self._repository.load_catalog_planning_state(operation_id=operation_id)
        operation = state.operation
        if (
            str(operation["status"]) != HistoricalRangeOperationStatus.COMPLETED.value
            or str(operation["catalog_phase"]) != HistoricalRangeCatalogPhase.VERIFY.value
            or int(operation["cumulative_resolved_count"]) != len(state.plan.requirements)
            or int(operation["cumulative_unresolved_count"]) != 0
        ):
            raise HistoricalRangeContractError(
                "ADVISORY_HR_CATALOG_NOT_SEALABLE",
                "catalog operation is not a completed VERIFY result",
                context={"operation_id": operation_id, "status": operation.get("status")},
            )
        return self._seal(operation_id=operation_id)

    def _publish_checkpoint(self, *, state: Any, checkpoint: HistoricalRangeSourceCatalogCheckpointV1):
        return self._artifact_store.publish_planning_payload(
            artifact_kind=HistoricalRangeArtifactKind.SOURCE_CATALOG_CHECKPOINT,
            planning_identity_hash=state.plan.planning_identity_hash,
            batch_id=state.plan.batch_id,
            catalog_generation=checkpoint.catalog_generation,
            producer_contract_version=PLANNING_PRODUCER_CONTRACT_VERSION,
            payload_schema_version=SOURCE_CATALOG_CHECKPOINT_SCHEMA_VERSION,
            payload=checkpoint.model_dump(mode="json"),
        ).ref

    @staticmethod
    def _drift_checkpoint(*, state: Any, ordinal: int, error: HistoricalRangeContractError):
        previous_ref, _previous = state.checkpoint_chain[-1]
        return HistoricalRangeSourceCatalogCheckpointV1(
            requirement_plan_hash=state.plan.requirement_plan_hash,
            catalog_generation=int(state.operation["catalog_generation"]),
            phase=HistoricalRangeCatalogPhase.VERIFY,
            ordinal_start=ordinal,
            ordinal_end=ordinal,
            next_requirement_ordinal=ordinal,
            previous_checkpoint_ref=previous_ref,
            previous_checkpoint_hash=previous_ref.semantic_content_hash,
            unresolved_requirement_delta=(
                HistoricalRangeUnresolvedRequirementV1(
                    ordinal=ordinal,
                    requirement_id=state.plan.requirements[ordinal - 1].requirement_id,
                    reason_code=error.reason_code,
                    context=error.context,
                ),
            ),
            cumulative_resolved_count=int(state.operation["cumulative_resolved_count"]),
            cumulative_member_chain_hash=str(state.operation["cumulative_member_chain_hash"]),
        )

    def _seal(self, *, operation_id: str) -> SealedHistoricalRangeBatch:
        state = self._repository.load_catalog_planning_state(operation_id=operation_id)
        plan = state.plan
        verify_members = {
            delta.member.requirement_id: delta.member
            for _ref, checkpoint in state.checkpoint_chain
            if checkpoint.phase is HistoricalRangeCatalogPhase.VERIFY
            for delta in checkpoint.member_delta
        }
        if set(verify_members) != {item.requirement_id for item in plan.requirements}:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_CATALOG_CHECKPOINT_CONFLICT",
                "completed VERIFY chain does not cover every planned requirement",
                context={"operation_id": operation_id},
            )
        catalog = HistoricalRangeSourceRevisionCatalogV1(
            requirement_plan_hash=str(plan.requirement_plan_hash),
            catalog_generation=int(state.operation["catalog_generation"]),
            query_contract_hash=plan.query_contract_hash,
            calendar_identity_hash=plan.calendar_identity_hash,
            members=tuple(verify_members.values()),
        )
        resolved_programs = self._bind_resolved_hmm_evidence(
            plan=plan,
            catalog=catalog,
            catalog_generation=int(state.operation["catalog_generation"]),
        )
        first_program = resolved_programs[0]
        resolved = ResolvedHistoricalRangeRequestV1(
            batch_id=plan.batch_id,
            request=plan.request,
            frozen_programs=resolved_programs,
            date_plan=plan.date_plan,
            source_revision_catalog_hash=str(catalog.catalog_hash),
            selection_semantics_version=first_program.selection_semantics_version,
            selection_semantics_hash=first_program.selection_semantics_hash,
            list_semantics_version=first_program.list_semantics_version,
            list_semantics_hash=first_program.list_semantics_hash,
        )
        payload = HistoricalRangeResolvedRequestArtifactPayloadV1(
            resolved_request=resolved,
            source_revision_catalog=catalog,
        )
        request_artifact = self._artifact_store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.REQUEST,
            producer_contract_version=PLANNING_PRODUCER_CONTRACT_VERSION,
            payload_schema_version=payload.schema_version,
            resolved_request_hash=str(resolved.request_payload_sha256),
            payload=payload.model_dump(mode="json"),
            source_revision_refs=catalog.source_revision_refs(),
        )
        date_artifact = self._artifact_store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.DATE_PLAN,
            producer_contract_version=PLANNING_PRODUCER_CONTRACT_VERSION,
            payload_schema_version=plan.date_plan.schema_version,
            resolved_request_hash=str(resolved.request_payload_sha256),
            payload=plan.date_plan.model_dump(mode="json"),
            upstream_refs=(request_artifact.ref,),
        )
        frozen_refs = {
            program.research_program_id: self._artifact_store.publish_payload(
                artifact_kind=HistoricalRangeArtifactKind.FROZEN_PROGRAM,
                producer_contract_version=PLANNING_PRODUCER_CONTRACT_VERSION,
                payload_schema_version=program.schema_version,
                resolved_request_hash=str(resolved.request_payload_sha256),
                range_run_id=resolved.range_run_id(program.research_program_id),
                payload=program.model_dump(mode="json"),
                upstream_refs=(request_artifact.ref, date_artifact.ref),
            ).ref
            for program in resolved_programs
        }
        bindings = HistoricalRangeArtifactBindingsV1(
            request_ref=request_artifact.ref,
            date_plan_ref=date_artifact.ref,
            frozen_program_refs=frozen_refs,
            artifact_root_identity_hash=self._artifact_store.root_identity_hash,
        )
        return self._repository.seal_planning_batch(
            batch_id=plan.batch_id,
            expected_row_version=int(state.batch["row_version"]),
            plan=plan,
            resolved=resolved,
            catalog=catalog,
            artifacts=bindings,
        )

    def _bind_resolved_hmm_evidence(
        self,
        *,
        plan: Any,
        catalog: HistoricalRangeSourceRevisionCatalogV1,
        catalog_generation: int,
    ) -> tuple[Any, ...]:
        members_by_program: dict[str, list[Any]] = {}
        for member in catalog.members:
            parameters = member.bound_parameters or {}
            selector = parameters.get("selector")
            research_program_id = selector.get("research_program_id") if isinstance(selector, dict) else None
            if member.source_role == "hmm_frozen_evidence" and research_program_id:
                members_by_program.setdefault(str(research_program_id), []).append(member)
        resolved_programs = []
        for program in plan.frozen_programs:
            members = members_by_program.get(program.research_program_id, [])
            expected_dates = {
                requirement.decision_trade_date
                for requirement in plan.requirements
                if requirement.source_role == "hmm_frozen_evidence"
                and requirement.package_id == program.package_id
                and (requirement.parameter_template.get("selector") or {}).get("research_program_id")
                == program.research_program_id
            }
            if not expected_dates:
                if members:
                    raise HistoricalRangeContractError(
                        "ADVISORY_HR_SOURCE_REVISION_MISMATCH",
                        "catalog contains unplanned HMM evidence for a Program",
                        context={"research_program_id": program.research_program_id},
                    )
                resolved_programs.append(program)
                continue
            actual_dates = {member.decision_trade_date for member in members}
            if None in expected_dates or None in actual_dates or actual_dates != expected_dates:
                raise HistoricalRangeContractError(
                    "ADVISORY_HR_HMM_FROZEN_EVIDENCE_UNAVAILABLE",
                    "resolved HMM evidence does not cover every planned decision day",
                    context={
                        "research_program_id": program.research_program_id,
                        "expected_dates": sorted(item.isoformat() for item in expected_dates if item is not None),
                        "actual_dates": sorted(item.isoformat() for item in actual_dates if item is not None),
                    },
                )
            bindings = []
            for member in members:
                parameters = member.bound_parameters or {}
                metadata = parameters.get("phase0a_hmm_metadata")
                if not isinstance(metadata, dict) or member.decision_trade_date is None:
                    raise HistoricalRangeContractError(
                        "ADVISORY_HR_HMM_FROZEN_EVIDENCE_UNAVAILABLE",
                        "resolved HMM catalog member has no exact metadata",
                        context={"revision_id": member.revision_id},
                    )
                bindings.append(
                    HistoricalRangeHMMBindingV1(
                        decision_trade_date=member.decision_trade_date,
                        phase0a_hmm_metadata=metadata,
                        source_revision_ref={
                            "revision_id": member.revision_id,
                            "revision_hash": member.revision_hash,
                        },
                    )
                )
            binding_set = HistoricalRangeHMMBindingSetV1(
                research_program_id=program.research_program_id,
                package_id=program.package_id,
                base_runtime_config_hash=program.runtime_config_hash,
                bindings=tuple(bindings),
            )
            stored = self._artifact_store.publish_planning_payload(
                artifact_kind=HistoricalRangeArtifactKind.HMM_BINDING_SET,
                planning_identity_hash=plan.planning_identity_hash,
                batch_id=plan.batch_id,
                catalog_generation=catalog_generation,
                producer_contract_version=PLANNING_PRODUCER_CONTRACT_VERSION,
                payload_schema_version=HMM_BINDING_SET_SCHEMA_VERSION,
                payload=binding_set.model_dump(mode="json"),
            )
            payload = program.model_dump(mode="json")
            payload["resolved_hmm_binding_set_ref"] = stored.ref.model_dump(mode="json")
            payload["resolved_hmm_binding_set_hash"] = binding_set.binding_set_hash
            payload["frozen_program_hash"] = None
            resolved_programs.append(type(program).model_validate(payload))
        return tuple(resolved_programs)
