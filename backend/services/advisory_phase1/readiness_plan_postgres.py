"""PostgreSQL-backed Phase 1E input provider using Advisory read-only projection."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Iterable

from backend.services.advisory_phase0a.evidence_projection_postgres import (
    AdvisoryPostgresEvidenceProjection,
    AdvisoryPostgresEvidenceSnapshot,
)
from backend.services.advisory_phase0a.evidence_projection import ProjectedSelectionScoreArtifact
from backend.services.advisory_phase0a.models import Phase0APolicyRegistry
from backend.services.advisory_phase0a.resolvers import AuditReaders
from backend.services.advisory_phase1.source_ledger import SourceAvailabilityEvent
from backend.services.advisory_phase1.source_resolution import SourceRequirementSet

from .readiness_plan import (
    Phase1EError,
    Phase1EInputProvider,
    Phase1EProgramDateEvidence,
    Phase1EProgramDateRequest,
    Phase1ERevalidationBatchRequest,
    REASON_DATED_BINDING_MISSING,
    REASON_HISTORICAL_RECEIPT_MISSING,
    REASON_PACKAGE_LINEAGE_HASH_MISMATCH,
)


class PostgresPhase1EInputProvider(Phase1EInputProvider):
    """Materialize one Program/date inside one REPEATABLE READ READ ONLY snapshot."""

    def __init__(self, *, projection: AdvisoryPostgresEvidenceProjection, policy: Phase0APolicyRegistry) -> None:
        self._projection = projection
        self._policy = policy
        self._context: AbstractContextManager[AdvisoryPostgresEvidenceSnapshot] | None = None
        self._snapshot: AdvisoryPostgresEvidenceSnapshot | None = None

    def resolve_program_date(
        self,
        *,
        request: Phase1EProgramDateRequest,
        batch_request: Phase1ERevalidationBatchRequest,
    ) -> Phase1EProgramDateEvidence:
        _ = batch_request
        self.close_program_date()
        context = self._projection.snapshot()
        snapshot = context.__enter__()
        self._context = context
        self._snapshot = snapshot
        try:
            pair = snapshot.get_historical_receipt(request.historical_batch_receipt_ref)
            if pair is None:
                raise Phase1EError(
                    REASON_HISTORICAL_RECEIPT_MISSING,
                    "historical batch receipt does not exist",
                    context={"historical_batch_receipt_ref": request.historical_batch_receipt_ref},
                )
            batch, receipt = pair
            program_run = next(
                (
                    item
                    for item in receipt.program_runs
                    if item.program_id == request.program_id and item.decision_trade_date == request.decision_trade_date
                ),
                None,
            )
            if program_run is None:
                raise Phase1EError(
                    REASON_HISTORICAL_RECEIPT_MISSING,
                    "historical receipt does not contain the requested Program/date",
                    context={"program_id": request.program_id, "decision_trade_date": request.decision_trade_date.isoformat()},
                )
            binding = self._dated_binding(snapshot=snapshot, program_id=request.program_id, decision_trade_date=request.decision_trade_date)
            if binding is None:
                raise Phase1EError(REASON_DATED_BINDING_MISSING, "dated Program binding is unavailable")
            package = snapshot.get(str(program_run.package_id)) if program_run.package_id else None
            selection_evidence = snapshot.get_daily_selection_evidence(str(program_run.evidence_id)) if program_run.evidence_id else None
            artifact = snapshot.get_selection_score_artifact(str(program_run.artifact_id)) if program_run.artifact_id else None
            if artifact is not None and (
                artifact.package_id != program_run.package_id
                or artifact.manifest_sha256 != program_run.manifest_sha256
                or artifact.artifact_payload_sha256 != program_run.artifact_payload_hash
            ):
                artifact = None
            if package is None or selection_evidence is None or artifact is None:
                raise Phase1EError(
                    REASON_PACKAGE_LINEAGE_HASH_MISMATCH,
                    "historical Program run does not resolve to exact immutable package/evidence/artifact rows",
                )
            readers = AuditReaders(
                advisory=snapshot,
                package=snapshot,
                evidence=snapshot,
                score_artifact=_PinnedSelectionScoreArtifactReader(artifact),
                selection_run=snapshot,
                source_probe=snapshot,
                calendar=snapshot,
            )
            return Phase1EProgramDateEvidence(
                historical_batch=batch,
                historical_receipt=receipt,
                historical_program_run=program_run,
                dated_binding=binding,
                package=package,
                selection_evidence=selection_evidence,
                selection_artifact=artifact,
                policy=self._policy,
                audit_readers=readers,
                postgres_now=snapshot.postgres_now(),
            )
        except Exception:
            self.close_program_date()
            raise

    def list_source_events(self, *, requirements: SourceRequirementSet) -> Iterable[SourceAvailabilityEvent]:
        snapshot = self._snapshot
        if snapshot is None:
            raise RuntimeError("Phase 1E source resolution was requested outside its read-only snapshot")
        events: dict[str, SourceAvailabilityEvent] = {}
        for requirement in requirements.requirements:
            for event in snapshot.list_source_events(
                dataset_name=requirement.dataset_name,
                source_role=requirement.source_role,
                partition_key=requirement.partition_key,
            ):
                events[event.event_content_hash] = event
        return tuple(events[key] for key in sorted(events))

    def close_program_date(self) -> None:
        context = self._context
        self._context = None
        self._snapshot = None
        if context is not None:
            context.__exit__(None, None, None)

    @staticmethod
    def _dated_binding(
        *,
        snapshot: AdvisoryPostgresEvidenceSnapshot,
        program_id: str,
        decision_trade_date,
    ) -> Any | None:
        candidates = []
        for binding in snapshot.list_binding_versions(program_id):
            if binding.activation_status == "DRAFT" or binding.effective_from_trade_date is None:
                continue
            if binding.effective_from_trade_date > decision_trade_date:
                continue
            if binding.effective_to_trade_date is not None and binding.effective_to_trade_date <= decision_trade_date:
                continue
            candidates.append(binding)
        return candidates[0] if len(candidates) == 1 else None


class _PinnedSelectionScoreArtifactReader:
    """Expose the exact historical artifact to the per-Program/date audit only."""

    def __init__(self, artifact: ProjectedSelectionScoreArtifact) -> None:
        self._artifact = artifact

    def list(
        self,
        *,
        package_id: str,
        manifest_sha256: str | None = None,
        limit: int = 100,
    ) -> list[ProjectedSelectionScoreArtifact]:
        if limit <= 0:
            return []
        if package_id != self._artifact.package_id:
            return []
        if manifest_sha256 is not None and manifest_sha256 != self._artifact.manifest_sha256:
            return []
        return [self._artifact]
