"""Production resolution processor for versioned monthly dataset requests.

This processor performs only read-only source acquisition and small control
plane writes.  It never launches helpers and never writes a candidate.  Exact
source/PIT streams are sealed in CAS before a NO_OP, REATTEST, or BUILD plan is
committed atomically through :class:`ResolutionService`.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

from .attestation import ATTESTATION_SCHEMA_VERSION, AttestationResult
from .canonical import digest_named_fields, ensure_sha256
from .cas_store import CASRef, CASStore
from .component_artifact_manifest import (
    ComponentArtifactManifest,
    load_component_artifact_manifest,
)
from .contracts import (
    AttestationIdentity,
    CandidateIdentity,
    Component,
    ComponentAction,
    EquivalenceMode,
    PitProvenanceState,
    ProducerProvenanceState,
    ResolvedIntentIdentity,
    Scope,
    SourceProbeSubjectKind,
    attestation_observation_key,
    canonical_request_hash,
)
from .control_store import ControlStore, SourceSnapshotCatalogSpec, StateConflict
from .decision import ActionPlan, ComponentPlan
from .errors import DatasetReleaseError, IdentityConflictError
from .mixed_planner import (
    MixedPlannerContext,
    build_mixed_action_plan,
    load_artifact_ready_planning_authority,
    pit_span_digest_by_code,
)
from .profile import DatasetProfile, load_initial_migration_plan
from .resolution import (
    BUILD_INPUTS_SCHEMA_VERSION,
    SourceProbeReceipt,
    SourceSnapshot,
    ResolutionService,
)
from .resource_budget import ResourceAdmissionClass
from .source_authority import (
    SOURCE_AUTHORITY_POLICY_VERSION,
    SOURCE_MANIFEST_ARTIFACT_SCHEMA,
    SOURCE_REUSE_MANIFEST_SCHEMA,
    FrozenSourceAuthoritySnapshot,
    MonthlySourceAuthority,
    load_source_stage_receipt,
)
from .state_machine import AttestationObservationSpec, AttestationRenewalSpec
from .worker import (
    WORKER_ERROR_RECEIPT_SCHEMA,
    ProcessorResult,
    WorkResourceSpec,
    WorkerAttemptContext,
)


MONTHLY_REQUEST_SCHEMA = "dataset_release_monthly_request_v1"
INITIAL_MIGRATION_REQUEST_SCHEMA = "dataset_release_initial_migration_request_v1"
REATTEST_REQUEST_SCHEMA = "dataset_release_reattest_request_v1"
SUBMISSION_REQUEST_SCHEMA = "dataset_release_submission_request_v1"
CANDIDATE_EVIDENCE_SCHEMA = "dataset_release_candidate_evidence_v1"
ATTESTATION_RENEWAL_SCHEMA = "dataset_release_attestation_renewal_v1"
MAX_REQUEST_BYTES = 2 * 1024 * 1024
MAX_CONTROL_ARTIFACT_BYTES = 32 * 1024 * 1024
MAX_CATALOG_ROWS = 100
MAX_ATTESTATION_ROWS = 100
MAX_SOURCE_STAGE_RESULT_BYTES = 64 * 1024
SAMPLE_POLICY = "on_contract_change"
SOURCE_STAGE_RESULT_SCHEMA = "dataset_release_source_stage_result_v1"
SOURCE_STAGE_ERROR_SCHEMA = "dataset_release_source_stage_error_v1"
GIB = 1024**3
# First-run admission is based on the versioned gzip-level1 sealed-source
# envelope.  The former 640 GiB value assumed the full 512 GiB source estimate
# stayed uncompressed.  We retain a deliberately conservative 0.50 codec ratio
# plus the independent 128 GiB candidate-output reserve.  Subsequent runs use
# exact compressed CAS sizes from the source snapshot catalog.
INITIAL_SOURCE_UNCOMPRESSED_ESTIMATE_BYTES = 512 * GIB
INITIAL_SOURCE_GZIP_ADMISSION_BYTES = INITIAL_SOURCE_UNCOMPRESSED_ESTIMATE_BYTES // 2
CANDIDATE_OUTPUT_PREDICTED_BYTES = 128 * GIB
INITIAL_SOURCE_AND_CANDIDATE_PREDICTED_BYTES = INITIAL_SOURCE_GZIP_ADMISSION_BYTES + CANDIDATE_OUTPUT_PREDICTED_BYTES

_MONTHLY_FIELDS = frozenset(
    {
        "schema_version",
        "profile",
        "cutoff_policy",
        "cutoff_resolution_policy",
        "resolved_cutoff",
        "scope",
        "candidate_only",
        "logical_request_key",
        "semantic_profile_digest",
        "resolution",
        "activation",
        "node1",
        "db_repair",
        "restart",
        "cleanup",
    }
)
_INITIAL_MIGRATION_FIELDS = frozenset(
    {
        "schema_version",
        "profile",
        "operation",
        "cutoff_policy",
        "resolved_cutoff",
        "scope",
        "candidate_only",
        "logical_request_key",
        "semantic_profile_digest",
        "plan_id",
        "plan_digest",
        "source_identity_policy",
        "sample_instruments",
        "event_windows",
        "index_windows",
        "plan_safety",
        "resolution",
        "activation",
        "node1",
        "db_repair",
        "restart",
        "cleanup",
    }
)
_REATTEST_FIELDS = frozenset(
    {
        "schema_version",
        "profile",
        "scope",
        "resolved_cutoff",
        "candidate_only",
        "logical_request_key",
        "semantic_profile_digest",
        "operation",
        "candidate_registration_id",
        "candidate_identity",
        "allowlisted_root_id",
        "volume_serial",
        "root_relative_path",
        "artifact_root",
        "lineage_anchor",
        "pit_provenance_state",
        "pit_provenance_digest_or_sentinel",
        "producer_provenance_state",
        "producer_provenance_digest_or_sentinel",
        "legacy_receipt_ref",
        "activation",
        "node1",
        "db_repair",
        "restart",
        "cleanup",
    }
)
_OUTER_FIELDS = frozenset({"schema_version", "logical_request_key", "request_hash", "request", "safety"})
_ZERO_SAFETY = {
    "database_writes": 0,
    "production_writes": 0,
    "production_deletes": 0,
    "production_pointer_changes": 0,
    "service_process_controls": 0,
}


class ResolutionProcessorError(DatasetReleaseError):
    code = "DATASET_RELEASE_RESOLUTION_PROCESSOR_ERROR"


class ResolutionSourceDriftWaiting(ResolutionProcessorError):
    code = "BLOCKED_SOURCE_SNAPSHOT_DRIFT"


class ResolutionRequestInvalid(ResolutionProcessorError):
    code = "BLOCKED_RESOLUTION_REQUEST_INVALID"


class ResolutionCatalogConflict(ResolutionProcessorError):
    code = "BLOCKED_RESOLUTION_CATALOG_CONFLICT"


class ResolutionCatalogIncomplete(ResolutionProcessorError):
    code = "BLOCKED_RESOLUTION_CATALOG_INCOMPLETE"


class ResolutionCandidateEvidenceInvalid(ResolutionProcessorError):
    code = "BLOCKED_RESOLUTION_CANDIDATE_EVIDENCE_INVALID"


class ResolutionResourceEvidenceInvalid(ResolutionProcessorError):
    code = "BLOCKED_RESOLUTION_RESOURCE_EVIDENCE_INVALID"


@dataclass(frozen=True, slots=True)
class VersionedResolutionRequest:
    schema_version: str
    profile: str
    scope: Scope
    cutoff: date
    logical_request_key: str
    semantic_profile_digest: str
    payload: Mapping[str, Any]

    @property
    def is_reattest(self) -> bool:
        return self.schema_version == REATTEST_REQUEST_SCHEMA

    @property
    def is_initial_migration(self) -> bool:
        return self.schema_version == INITIAL_MIGRATION_REQUEST_SCHEMA

    @property
    def sample_instruments(self) -> tuple[str, ...]:
        if not self.is_initial_migration or self.scope is not Scope.SAMPLE:
            return ()
        return tuple(str(value) for value in self.payload["sample_instruments"])


@dataclass(frozen=True, slots=True)
class CatalogCandidate:
    registration_id: str
    allowlisted_root_id: str
    volume_serial: str
    root_relative_path: str
    profile: str
    scope: Scope
    cutoff: date
    lineage_anchor: str
    candidate_identity: str
    artifact_root: str
    producer_provenance_state: ProducerProvenanceState
    producer_provenance_digest_or_sentinel: str
    pit_provenance_state: PitProvenanceState
    pit_provenance_digest_or_sentinel: str
    legacy_receipt_ref: str | None
    state: str
    observed_at: datetime
    build_receipt_ref: str | None = None
    release_id: str | None = None
    release_digest: str | None = None
    attestation_key: str | None = None

    @property
    def identity(self) -> CandidateIdentity:
        value = CandidateIdentity(
            registration_uuid=self.registration_id,
            allowlisted_root_id=self.allowlisted_root_id,
            volume_serial=self.volume_serial,
            root_relative_path=self.root_relative_path,
            profile=self.profile,
            scope=self.scope,
            cutoff=self.cutoff,
            lineage_anchor=self.lineage_anchor,
            pit_provenance_state=self.pit_provenance_state,
            pit_provenance_digest_or_sentinel=self.pit_provenance_digest_or_sentinel,
            artifact_root=self.artifact_root,
            producer_provenance_state=self.producer_provenance_state,
            producer_provenance_digest_or_sentinel=(self.producer_provenance_digest_or_sentinel),
        )
        if value.key != self.candidate_identity:
            raise ResolutionCatalogConflict(
                "catalog candidate identity does not match its immutable fields",
                context={"registration_id": self.registration_id},
            )
        return value


@dataclass(frozen=True, slots=True)
class CandidateSourceEvidence:
    source_manifest_ref: CASRef
    source_content_root: str
    partitions: tuple[Mapping[str, Any], ...]
    raw_source_content_root: str
    source_reuse_manifest_ref: CASRef | None = None
    reuse_partitions: tuple[Mapping[str, Any], ...] = ()
    component_artifact_manifest_ref: CASRef | None = None
    component_artifact_manifest: ComponentArtifactManifest | None = None


@dataclass(frozen=True, slots=True)
class SourceReuseBaseline:
    source_reuse_manifest_ref: CASRef
    partitions: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True, slots=True)
class FreshAttestation:
    result: AttestationResult
    renewal: AttestationRenewalSpec | None


class ResolutionSourceStage(Protocol):
    def freeze(
        self,
        context: WorkerAttemptContext,
        *,
        cutoff: date,
        baseline_reuse_ref: CASRef | None,
        baseline_partitions: Sequence[Mapping[str, Any]],
        predicted_new_bytes: int,
        pressure_rung: int,
        sample_instruments: Sequence[str],
    ) -> FrozenSourceAuthoritySnapshot: ...


class InProcessFixtureSourceStage:
    """Explicit fixture-only adapter; production construction never selects it."""

    def __init__(self, authority: MonthlySourceAuthority) -> None:
        self.authority = authority

    def freeze(
        self,
        context: WorkerAttemptContext,
        *,
        cutoff: date,
        baseline_reuse_ref: CASRef | None,
        baseline_partitions: Sequence[Mapping[str, Any]],
        predicted_new_bytes: int,
        pressure_rung: int,
        sample_instruments: Sequence[str],
    ) -> FrozenSourceAuthoritySnapshot:
        del baseline_reuse_ref, predicted_new_bytes
        return self.authority.freeze(
            cutoff=cutoff,
            checkpoint=context.checkpoint,
            baseline_partitions=baseline_partitions,
            pressure_rung=pressure_rung,
            sample_instruments=sample_instruments,
        )


class SupervisedResolutionSourceStage:
    """Production source scan boundary: all data-bearing reads run as a child."""

    def __init__(
        self,
        profile: DatasetProfile,
        store: ControlStore,
        cas: CASStore,
        *,
        project_root: Path | None = None,
        python_executable: str | None = None,
    ) -> None:
        self.profile = profile
        self.store = store
        self.cas = cas
        self.project_root = (project_root or Path(__file__).resolve().parents[3]).resolve(strict=True)
        self.python_executable = python_executable or sys.executable
        self.script = self.project_root / "scripts" / "dataset_release_source_stage.py"
        if not self.script.is_file():
            raise ResolutionProcessorError("supervised source-stage script is missing")

    def freeze(
        self,
        context: WorkerAttemptContext,
        *,
        cutoff: date,
        baseline_reuse_ref: CASRef | None,
        baseline_partitions: Sequence[Mapping[str, Any]],
        predicted_new_bytes: int,
        pressure_rung: int,
        sample_instruments: Sequence[str],
    ) -> FrozenSourceAuthoritySnapshot:
        if baseline_partitions and baseline_reuse_ref is None:
            raise ResolutionCandidateEvidenceInvalid(
                "baseline reuse rows lack their immutable reuse-manifest reference"
            )
        execution_id = "resolution-source-freeze"
        execution_root = (
            self.store.root
            / "attempt_runs"
            / f"{context.claim.attempt_id}-{context.claim.attempt_fence}"
            / execution_id
        )
        result_path = execution_root / "semantic_result.json"
        command = [
            self.python_executable,
            str(self.script),
            "--profile",
            str(self.profile.path),
            "--control-root",
            str(self.store.root),
            "--cutoff",
            cutoff.isoformat(),
            "--attempt-id",
            context.claim.attempt_id,
            "--attempt-fence",
            str(context.claim.attempt_fence),
            "--execution-id",
            execution_id,
            "--result-path",
            str(result_path),
            "--predicted-new-bytes",
            str(predicted_new_bytes),
            "--pressure-rung",
            str(pressure_rung),
            "--stage-timeout-seconds",
            str(self.profile.stage_timeouts_seconds["source_freeze"]),
        ]
        for instrument in sample_instruments:
            command.extend(("--sample-instrument", instrument))
        if baseline_reuse_ref is not None:
            command.extend(("--baseline-reuse-ref", baseline_reuse_ref.sha256))
        receipt = context.run_supervised(
            command,
            execution_id=execution_id,
            cwd=self.project_root,
            runtime="windows",
            timeout_seconds=float(self.profile.stage_timeouts_seconds["source_freeze"]),
            cooperative_grace_seconds=30.0,
        )
        if receipt.active_processes != 0 or receipt.runtime != "windows":
            raise ResolutionProcessorError(
                "supervised source stage failed",
                context={
                    "returncode": receipt.returncode,
                    "active_processes": receipt.active_processes,
                    "runtime": receipt.runtime,
                },
            )
        if receipt.returncode != 0:
            if result_path.is_file():
                error = _read_bounded_source_stage_result(result_path, control_root=self.store.root)
                if _is_waitable_source_drift(error):
                    raise ResolutionSourceDriftWaiting(
                        "supervised source stage observed transient source drift",
                        context={
                            "source_stage_error_code": error["error_code"],
                            "source_stage_exception_type": error["exception_type"],
                            "source_stage_message_sha256": error["message_sha256"],
                        },
                    )
            raise ResolutionProcessorError(
                "supervised source stage failed",
                context={
                    "returncode": receipt.returncode,
                    "active_processes": receipt.active_processes,
                    "runtime": receipt.runtime,
                },
            )
        result = _read_bounded_source_stage_result(result_path, control_root=self.store.root)
        if (
            result.get("schema_version") != SOURCE_STAGE_RESULT_SCHEMA
            or result.get("stage_timeout_seconds") != self.profile.stage_timeouts_seconds["source_freeze"]
            or result.get("safety") != {**_ZERO_SAFETY, "provider_database_writes": 0, "candidate_writes": 0}
        ):
            raise ResolutionProcessorError("supervised source-stage result differs")
        return load_source_stage_receipt(
            self.cas,
            result.get("source_stage_receipt_ref"),
            expected_profile=self.profile.profile,
            expected_cutoff=cutoff,
            profile=self.profile,
        )


class MonthlyResolutionProcessor:
    """Default Worker ``ResolutionProcessor`` implementation."""

    def __init__(
        self,
        profile: DatasetProfile,
        store: ControlStore,
        cas: CASStore,
        *,
        source_authority: MonthlySourceAuthority | None = None,
        source_stage: ResolutionSourceStage | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        producer_fingerprint: str | None = None,
        artifact_fingerprint: str | None = None,
        validation_fingerprint: str | None = None,
    ) -> None:
        self.profile = profile
        self.store = store
        self.cas = cas
        if source_authority is not None and source_stage is not None:
            raise ResolutionProcessorError(
                "source_authority fixture and production source_stage are mutually exclusive"
            )
        self.source_stage: ResolutionSourceStage = source_stage or (
            InProcessFixtureSourceStage(source_authority)
            if source_authority is not None
            else SupervisedResolutionSourceStage(profile, store, cas)
        )
        self._now = now
        self.producer_fingerprint = producer_fingerprint or digest_named_fields(
            "dataset_release_monthly_producer_contract_v1",
            {
                "profile": profile.profile,
                "semantic_profile_digest": profile.semantic_profile_digest,
                "source_authority_policy": SOURCE_AUTHORITY_POLICY_VERSION,
                "components": [item.value for item in profile.components],
                "qlib_toolchain_profile_digest": profile.qlib_toolchain.digest,
                "qlib_dump_script_sha256": (profile.qlib_toolchain.dump_script_sha256),
            },
        )
        self.artifact_fingerprint = artifact_fingerprint or digest_named_fields(
            "dataset_release_monthly_artifact_contract_v1",
            {
                "profile": profile.profile,
                "semantic_profile_digest": profile.semantic_profile_digest,
                "moneyflow_contract": profile.moneyflow_contract,
                "static_column_count": profile.static_column_count,
                "qlib_stock_schema_digest": profile.qlib_stock_schema_digest,
                "index_codes": list(profile.index_codes),
            },
        )
        self.validation_fingerprint = validation_fingerprint or digest_named_fields(
            "dataset_release_monthly_validation_contract_v1",
            {
                "profile": profile.profile,
                "semantic_profile_digest": profile.semantic_profile_digest,
                "required_components": [item.value for item in profile.components],
                "sample_policy": SAMPLE_POLICY,
            },
        )
        for field in (
            "producer_fingerprint",
            "artifact_fingerprint",
            "validation_fingerprint",
        ):
            ensure_sha256(getattr(self, field), field=field)

    def resource_spec(self, submission: Mapping[str, Any]) -> WorkResourceSpec:
        if str(submission.get("logical_request_key", "")) == "":
            raise ResolutionRequestInvalid("resolution submission lacks logical identity")
        request = self._read_request(submission)
        return WorkResourceSpec(
            policy=self.profile.resource_policy,
            hybrid_wsl=False,
            release_id=None,
            acquire_host=True,
            db_connections=1,
            io_class="dataset-release-resolution-readonly",
            pressure_rung=self._resume_pressure_rung(str(submission.get("submission_id", ""))),
            predicted_new_bytes=self._predicted_new_bytes(submission),
            credential_env_allowlist=(
                "TDX_DB_HOST",
                "TDX_DB_NAME",
                "TDX_DB_PASSWORD",
                "TDX_DB_PORT",
                "TDX_DB_USER",
                "TUSHARE_TOKEN",
            ),
            admission_class=(
                ResourceAdmissionClass.RESOLUTION_LIGHT
                if request.scope is Scope.SAMPLE
                else ResourceAdmissionClass.FULL
            ),
        )

    def _predicted_new_bytes(self, submission: Mapping[str, Any]) -> int:
        request = self._read_request(submission)
        try:
            baseline = self.store.latest_source_snapshot(
                profile=request.profile,
                scope=request.scope.value,
                cutoff_on_or_before=request.cutoff,
            )
        except StateConflict as exc:
            raise ResolutionCatalogConflict("source snapshot capacity baseline is ambiguous") from exc
        if baseline is None:
            return INITIAL_SOURCE_AND_CANDIDATE_PREDICTED_BYTES
        reference = self.cas.verify(str(baseline["source_reuse_manifest_ref"]))
        receipt = self.cas.get_json_bounded(reference, max_bytes=MAX_CONTROL_ARTIFACT_BYTES)
        if not isinstance(receipt, Mapping) or not isinstance(receipt.get("partitions"), list):
            raise ResolutionCandidateEvidenceInvalid("source capacity baseline manifest is invalid")
        total = 0
        for raw in receipt["partitions"]:
            if not isinstance(raw, Mapping):
                raise ResolutionCandidateEvidenceInvalid("source capacity baseline partition is invalid")
            reference_value = _complete_cas_ref(self.cas, raw.get("rows_ref"), field="capacity.rows_ref")
            total += reference_value.size
        return total + CANDIDATE_OUTPUT_PREDICTED_BYTES

    def _resume_pressure_rung(self, submission_id: str) -> int:
        if not submission_id:
            raise ResolutionResourceEvidenceInvalid("resolution submission id is missing for pressure recovery")
        attempts = self.store._many(
            """
            SELECT ordinal,error_ref FROM resolution_attempts
            WHERE submission_id=? AND state='RELEASED_WAITING'
              AND error_ref IS NOT NULL
            ORDER BY ordinal
            """,
            (submission_id,),
        )
        admissions = self.store._many(
            """
            SELECT event_id,payload_ref FROM events
            WHERE submission_id=? AND type='RESOURCE_WAITING_SOURCE'
              AND payload_ref IS NOT NULL
            ORDER BY event_id
            """,
            (submission_id,),
        )
        sequences: list[list[int]] = [[], []]
        for sequence, rows in zip(sequences, (admissions, attempts)):
            for row in rows:
                sequence.append(self._pressure_rung_from_receipt(submission_id, row))
        if any(later < earlier for sequence in sequences for earlier, later in zip(sequence, sequence[1:])):
            raise ResolutionResourceEvidenceInvalid("durable resolution pressure rung moved backwards")
        latest = [sequence[-1] for sequence in sequences if sequence]
        return max(latest) if latest else 0

    def _pressure_rung_from_receipt(
        self,
        submission_id: str,
        row: Mapping[str, Any],
    ) -> int:
        reference = str(row.get("payload_ref") or row.get("error_ref") or "")
        receipt = self.cas.get_json_bounded(reference, max_bytes=1024 * 1024)
        if (
            not isinstance(receipt, Mapping)
            or receipt.get("schema_version") != WORKER_ERROR_RECEIPT_SCHEMA
            or receipt.get("target_id") != submission_id
            or receipt.get("disposition") != "WAITING"
            or receipt.get("kind") not in {"resolution", "resolution_resource_admission"}
        ):
            raise ResolutionResourceEvidenceInvalid("durable resolution pressure-rung receipt identity is invalid")
        context = receipt.get("context")
        if (
            not isinstance(context, Mapping)
            or context.get("data_scope_changed") is not False
            or type(context.get("pressure_rung")) is not int
        ):
            raise ResolutionResourceEvidenceInvalid("durable resolution pressure-rung receipt context is invalid")
        rung = int(context["pressure_rung"])
        if not 0 <= rung < len(self.profile.pressure_ladder["h5_batch"]):
            raise ResolutionResourceEvidenceInvalid("durable resolution pressure rung exceeds the profile ladder")
        return rung

    def process(self, context: WorkerAttemptContext) -> ProcessorResult:
        if context.kind != "resolution" or context.store.root != self.store.root:
            raise ResolutionRequestInvalid("resolution processor context is bound to a different control store")
        context.checkpoint()
        request = self._read_request(context.record)
        candidates = self._bounded_catalog(request)
        candidate = self._select_candidate(request, candidates)
        evidence = self._candidate_source_evidence(candidate)
        catalog_baseline = self._source_reuse_baseline(request)
        baseline_ref = (
            catalog_baseline.source_reuse_manifest_ref
            if catalog_baseline is not None
            else (evidence.source_reuse_manifest_ref if evidence else None)
        )
        baseline_partitions = (
            catalog_baseline.partitions
            if catalog_baseline is not None
            else (evidence.reuse_partitions if evidence else ())
        )
        predicted_new_bytes = self._predicted_new_bytes(context.record)
        context.checkpoint()
        try:
            frozen = self.source_stage.freeze(
                context,
                cutoff=request.cutoff,
                baseline_reuse_ref=baseline_ref,
                baseline_partitions=baseline_partitions,
                predicted_new_bytes=predicted_new_bytes,
                pressure_rung=context.pressure_rung,
                sample_instruments=request.sample_instruments,
            )
        except ResolutionSourceDriftWaiting as exc:
            return ProcessorResult.waiting(
                exc.code,
                context={
                    **exc.context,
                    "pressure_rung": context.pressure_rung,
                    "data_scope_changed": False,
                },
            )
        context.checkpoint()
        probe = self._record_probe(context, request, frozen, candidate)
        catalog_spec = self._source_snapshot_catalog_spec(request, frozen, probe)
        service = ResolutionService(self.store, self.cas)
        fresh_attestation = self._fresh_attestation(candidate, probe)
        if fresh_attestation is not None:
            service.resolve_noop(
                submission_id=context.target_id,
                claim=context.claim,
                probe=probe,
                attestation=fresh_attestation.result,
                producer_fingerprint=self.producer_fingerprint,
                artifact_fingerprint=self.artifact_fingerprint,
                sample_policy=SAMPLE_POLICY,
                source_snapshot_catalog=catalog_spec,
                attestation_renewal=fresh_attestation.renewal,
                now=self._aware_now(),
            )
            return ProcessorResult.durable_success()

        if candidate is not None and self._should_reattest(
            request,
            candidate,
            evidence,
            frozen,
        ):
            action_plan, target = self._reattest_plan(candidate, probe, frozen)
            service.resolve_action_plan(
                submission_id=context.target_id,
                claim=context.claim,
                probe=probe,
                action_plan=action_plan,
                producer_fingerprint=self.producer_fingerprint,
                artifact_fingerprint=self.artifact_fingerprint,
                validation_identity=target.key,
                sample_policy=SAMPLE_POLICY,
                attestation_target_key=target.target_key,
                source_snapshot_catalog=catalog_spec,
                now=self._aware_now(),
            )
            return ProcessorResult.durable_success()

        action_plan = self._build_plan(candidate, evidence, frozen)
        build_inputs = self._build_inputs(
            request=request,
            candidate=candidate,
            evidence=evidence,
            frozen=frozen,
            probe=probe,
            action_plan=action_plan,
        )
        service.resolve_action_plan(
            submission_id=context.target_id,
            claim=context.claim,
            probe=probe,
            action_plan=action_plan,
            producer_fingerprint=self.producer_fingerprint,
            artifact_fingerprint=self.artifact_fingerprint,
            validation_identity=self.validation_fingerprint,
            sample_policy=SAMPLE_POLICY,
            build_inputs=build_inputs,
            source_snapshot_catalog=catalog_spec,
            now=self._aware_now(),
        )
        return ProcessorResult.durable_success()

    def _read_request(self, submission: Mapping[str, Any]) -> VersionedResolutionRequest:
        outer = self.cas.get_json_bounded(str(submission.get("request_ref", "")), max_bytes=MAX_REQUEST_BYTES)
        if not isinstance(outer, Mapping) or set(outer) != _OUTER_FIELDS:
            raise ResolutionRequestInvalid("submission request envelope is invalid")
        if outer.get("schema_version") != SUBMISSION_REQUEST_SCHEMA:
            raise ResolutionRequestInvalid("submission request envelope version is invalid")
        if dict(outer.get("safety") or {}) != _ZERO_SAFETY:
            raise ResolutionRequestInvalid("submission request safety boundary is invalid")
        inner = outer.get("request")
        if not isinstance(inner, Mapping):
            raise ResolutionRequestInvalid("submission request payload is not a mapping")
        schema = str(inner.get("schema_version", ""))
        expected_fields = {
            MONTHLY_REQUEST_SCHEMA: _MONTHLY_FIELDS,
            INITIAL_MIGRATION_REQUEST_SCHEMA: _INITIAL_MIGRATION_FIELDS,
            REATTEST_REQUEST_SCHEMA: _REATTEST_FIELDS,
        }.get(schema)
        if expected_fields is None or set(inner) != expected_fields:
            raise ResolutionRequestInvalid(
                "submission request payload schema/fields are invalid",
                context={"schema_version": schema},
            )
        logical = ensure_sha256(str(inner.get("logical_request_key", "")), field="logical_request_key")
        if (
            outer.get("logical_request_key") != logical
            or submission.get("logical_request_key") != logical
            or outer.get("request_hash") != canonical_request_hash(inner)
        ):
            raise ResolutionRequestInvalid("submission request identity/hash differs")
        if (
            inner.get("profile") != self.profile.profile
            or inner.get("semantic_profile_digest") != self.profile.semantic_profile_digest
            or inner.get("candidate_only") is not True
        ):
            raise ResolutionRequestInvalid("submission request profile contract differs")
        for field in ("activation", "node1", "db_repair", "restart", "cleanup"):
            if inner.get(field) != "not_requested":
                raise ResolutionRequestInvalid(
                    "submission request contains a forbidden production action",
                    context={"field": field},
                )
        if schema == MONTHLY_REQUEST_SCHEMA and (
            inner.get("cutoff_policy") != "auto-previous-month"
            or inner.get("cutoff_resolution_policy") != "previous_month_last_completed_trading_day"
            or inner.get("resolution") != "worker_required"
        ):
            raise ResolutionRequestInvalid("monthly cutoff/resolution policy differs")
        if schema == REATTEST_REQUEST_SCHEMA and inner.get("operation") != "reattest-existing":
            raise ResolutionRequestInvalid("re-attestation operation is invalid")
        try:
            cutoff = date.fromisoformat(str(inner.get("resolved_cutoff")))
            scope = Scope(str(inner.get("scope")))
        except ValueError as exc:
            raise ResolutionRequestInvalid("request cutoff/scope is invalid") from exc
        if schema == INITIAL_MIGRATION_REQUEST_SCHEMA:
            self._validate_initial_migration_request(inner, cutoff=cutoff, scope=scope)
        return VersionedResolutionRequest(
            schema_version=schema,
            profile=self.profile.profile,
            scope=scope,
            cutoff=cutoff,
            logical_request_key=logical,
            semantic_profile_digest=self.profile.semantic_profile_digest,
            payload=dict(inner),
        )

    def _validate_initial_migration_request(
        self,
        value: Mapping[str, Any],
        *,
        cutoff: date,
        scope: Scope,
    ) -> None:
        if (
            value.get("operation") != "initial-migration"
            or value.get("cutoff_policy") != "fixed-allowlisted-plan"
            or value.get("resolution") != "worker_required"
        ):
            raise ResolutionRequestInvalid("initial migration operation/cutoff policy differs")
        plan_id = str(value.get("plan_id") or "")
        if plan_id not in self.profile.initial_migration_plan_ids:
            raise ResolutionRequestInvalid("initial migration plan is not profile-allowlisted")
        try:
            plan = load_initial_migration_plan(self.profile.path.parent / "migrations" / f"{plan_id}.yaml")
            plan_digest = ensure_sha256(str(value.get("plan_digest") or ""), field="plan_digest")
        except (DatasetReleaseError, OSError, ValueError) as exc:
            raise ResolutionRequestInvalid("initial migration plan cannot be verified") from exc
        expected = {
            "profile": plan.profile,
            "resolved_cutoff": plan.cutoff.isoformat(),
            "plan_id": plan.plan_id,
            "plan_digest": plan.plan_digest,
            "source_identity_policy": plan.source_identity_policy,
            "sample_instruments": list(plan.sample_instruments),
            "event_windows": [dict(item) for item in plan.event_windows],
            "index_windows": [dict(item) for item in plan.index_windows],
            "plan_safety": dict(plan.raw["safety"]),
        }
        expected_logical_request_key = digest_named_fields(
            "dataset_release_initial_migration_logical_request_v1",
            {
                "profile": plan.profile,
                "scope": scope.value,
                "cutoff": plan.cutoff,
                "semantic_profile_digest": self.profile.semantic_profile_digest,
                "plan_id": plan.plan_id,
                "plan_digest": plan.plan_digest,
            },
        )
        mismatches = {
            field: {"expected": expected_value, "actual": value.get(field)}
            for field, expected_value in expected.items()
            if value.get(field) != expected_value
        }
        if (
            mismatches
            or plan_digest != plan.plan_digest
            or cutoff != plan.cutoff
            or not plan.allows_scope(scope.value)
            or value.get("logical_request_key") != expected_logical_request_key
        ):
            raise ResolutionRequestInvalid(
                "initial migration request differs from the checked-in plan",
                context={
                    "mismatches": mismatches,
                    "scope": scope.value,
                    "logical_request_key_matches": value.get("logical_request_key") == expected_logical_request_key,
                },
            )

    def _bounded_catalog(
        self,
        request: VersionedResolutionRequest,
    ) -> tuple[CatalogCandidate, ...]:
        registrations = self.store.list_candidate_registrations(
            profile=request.profile,
            scope=request.scope.value,
            limit=MAX_CATALOG_ROWS,
        )
        releases = self._committed_release_rows(request)
        release_by_candidate = {str(row["candidate_identity"]): row for row in releases}
        candidates: dict[str, CatalogCandidate] = {}
        for row in registrations:
            release = release_by_candidate.pop(str(row["candidate_identity"]), None)
            candidate = self._catalog_candidate(row, release=release)
            candidate.identity
            candidates[candidate.candidate_identity] = candidate
        for release in release_by_candidate.values():
            receipt_ref = str(release.get("build_receipt_ref") or "")
            receipt = self.cas.get_json_bounded(
                receipt_ref,
                max_bytes=MAX_CONTROL_ARTIFACT_BYTES,
            )
            if not isinstance(receipt, Mapping) or not isinstance(receipt.get("candidate_registration"), Mapping):
                raise ResolutionCatalogIncomplete(
                    "committed release lacks complete candidate registration evidence",
                    context={"release_id": release.get("release_id")},
                )
            candidate = self._catalog_candidate(dict(receipt["candidate_registration"]), release=release)
            candidate.identity
            candidates[candidate.candidate_identity] = candidate
        values = sorted(
            candidates.values(),
            key=lambda item: (
                item.cutoff,
                item.observed_at,
                item.artifact_root,
                item.candidate_identity,
            ),
            reverse=True,
        )
        if len(values) > MAX_CATALOG_ROWS:
            raise ResolutionCatalogConflict("candidate catalog exceeds its read bound")
        if len(values) >= 2 and _catalog_rank(values[0]) == _catalog_rank(values[1]):
            raise ResolutionCatalogConflict(
                "highest candidate catalog entries are ambiguous",
                context={
                    "candidate_identities": [
                        values[0].candidate_identity,
                        values[1].candidate_identity,
                    ]
                },
            )
        return tuple(values)

    def _committed_release_rows(
        self,
        request: VersionedResolutionRequest,
    ) -> list[dict[str, Any]]:
        with self.store.transaction(immediate=False) as connection:
            rows = connection.execute(
                """
                SELECT r.*,p.build_receipt_ref,p.marker_ref AS publish_marker_ref,
                       p.state AS publish_state
                FROM releases r
                JOIN publish_records p ON p.run_id=r.run_id
                WHERE r.profile=? AND r.scope=? AND r.state='COMMITTED'
                  AND p.state='COMMITTED' AND p.marker_ref IS NOT NULL
                ORDER BY r.cutoff DESC,r.artifact_root DESC,r.candidate_identity
                LIMIT ?
                """,
                (request.profile, request.scope.value, MAX_CATALOG_ROWS + 1),
            ).fetchall()
        if len(rows) > MAX_CATALOG_ROWS:
            raise ResolutionCatalogConflict("committed release catalog exceeds its read bound")
        return [dict(row) for row in rows]

    def _catalog_candidate(
        self,
        row: Mapping[str, Any],
        *,
        release: Mapping[str, Any] | None,
    ) -> CatalogCandidate:
        try:
            candidate_identity = ensure_sha256(str(row["candidate_identity"]), field="candidate_identity")
            artifact_root = ensure_sha256(str(row["artifact_root"]), field="artifact_root")
            cutoff = date.fromisoformat(str(row["cutoff"]))
            scope = Scope(str(row["scope"]))
            producer_state = ProducerProvenanceState(str(row["producer_provenance_state"]))
            pit_state = PitProvenanceState(str(row["pit_provenance_state"]))
        except (KeyError, ValueError) as exc:
            raise ResolutionCatalogIncomplete("candidate catalog row is incomplete") from exc
        latest_attestation = self._latest_attestation_row(candidate_identity)
        observed_raw = (
            row.get("last_attested_at")
            or (latest_attestation or {}).get("observed_at")
            or row.get("created_at")
            or "1970-01-01T00:00:00+00:00"
        )
        return CatalogCandidate(
            registration_id=str(row["registration_id"]),
            allowlisted_root_id=str(row["allowlisted_root_id"]),
            volume_serial=str(row["volume_serial"]),
            root_relative_path=str(row["root_relative_path"]),
            profile=str(row["profile"]),
            scope=scope,
            cutoff=cutoff,
            lineage_anchor=str(row["lineage_anchor"]),
            candidate_identity=candidate_identity,
            artifact_root=artifact_root,
            producer_provenance_state=producer_state,
            producer_provenance_digest_or_sentinel=str(row["producer_provenance_digest_or_sentinel"]),
            pit_provenance_state=pit_state,
            pit_provenance_digest_or_sentinel=str(row["pit_provenance_digest_or_sentinel"]),
            legacy_receipt_ref=(str(row["legacy_receipt_ref"]) if row.get("legacy_receipt_ref") is not None else None),
            state=str(row.get("state") or "RELEASED"),
            observed_at=_parse_utc(observed_raw),
            build_receipt_ref=(str(release["build_receipt_ref"]) if release is not None else None),
            release_id=str(release["release_id"]) if release is not None else None,
            release_digest=(
                ensure_sha256(str(release["release_digest"]), field="release_digest") if release is not None else None
            ),
            attestation_key=(str(latest_attestation["attestation_key"]) if latest_attestation is not None else None),
        )

    def _select_candidate(
        self,
        request: VersionedResolutionRequest,
        candidates: Sequence[CatalogCandidate],
    ) -> CatalogCandidate | None:
        if request.is_reattest:
            registration_id = str(request.payload["candidate_registration_id"])
            matches = [item for item in candidates if item.registration_id == registration_id]
            if len(matches) != 1:
                raise ResolutionCatalogConflict("re-attestation request candidate is missing or ambiguous")
            candidate = matches[0]
            expected = {
                "profile": candidate.profile,
                "scope": candidate.scope.value,
                "resolved_cutoff": candidate.cutoff.isoformat(),
                "candidate_registration_id": candidate.registration_id,
                "candidate_identity": candidate.candidate_identity,
                "allowlisted_root_id": candidate.allowlisted_root_id,
                "volume_serial": candidate.volume_serial,
                "root_relative_path": candidate.root_relative_path,
                "artifact_root": candidate.artifact_root,
                "lineage_anchor": candidate.lineage_anchor,
                "pit_provenance_state": candidate.pit_provenance_state.value,
                "pit_provenance_digest_or_sentinel": (candidate.pit_provenance_digest_or_sentinel),
                "producer_provenance_state": (candidate.producer_provenance_state.value),
                "producer_provenance_digest_or_sentinel": (candidate.producer_provenance_digest_or_sentinel),
                "legacy_receipt_ref": candidate.legacy_receipt_ref,
            }
            mismatch = {
                field: {"expected": value, "actual": request.payload.get(field)}
                for field, value in expected.items()
                if request.payload.get(field) != value
            }
            if mismatch:
                raise ResolutionCatalogConflict(
                    "re-attestation request differs from the exact catalog row",
                    context=mismatch,
                )
            return candidate
        eligible = [item for item in candidates if item.cutoff <= request.cutoff]
        return eligible[0] if eligible else None

    def _record_probe(
        self,
        context: WorkerAttemptContext,
        request: VersionedResolutionRequest,
        frozen: FrozenSourceAuthoritySnapshot,
        candidate: CatalogCandidate | None,
    ) -> SourceProbeReceipt:
        attempt = self.store.get_resolution_attempt(context.claim.attempt_id)
        if attempt is None:
            raise ResolutionRequestInvalid("resolution attempt disappeared")
        current = attempt.get("source_probe_ordinal")
        ordinal = 1 if current is None else int(current) + 1
        subject_kind = (
            SourceProbeSubjectKind.CATALOG_CANDIDATE if candidate is not None else SourceProbeSubjectKind.NEW_BUILD
        )
        effective_root, effective_provenance = _artifact_ready_roots(frozen)
        return ResolutionService(self.store, self.cas).record_source_probe(
            submission_id=context.target_id,
            claim=context.claim,
            candidate_identity=(candidate.candidate_identity if candidate else None),
            artifact_root=(candidate.artifact_root if candidate else None),
            snapshot=SourceSnapshot(
                source_content_root=effective_root,
                source_provenance_root=effective_provenance,
                pit_snapshot_digest=frozen.pit_snapshot_digest,
                snapshot_tokens=frozen.snapshot_tokens,
            ),
            probe_policy_version=SOURCE_AUTHORITY_POLICY_VERSION,
            probe_ordinal=ordinal,
            observed_at=self._aware_now(),
            ttl=timedelta(seconds=self.profile.source_content_probe_ttl_seconds),
            subject_kind=subject_kind,
        )

    def _fresh_attestation(
        self,
        candidate: CatalogCandidate | None,
        probe: SourceProbeReceipt,
    ) -> FreshAttestation | None:
        if candidate is None:
            return None
        observed = self._aware_now()
        with self.store.transaction(immediate=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM attestations
                WHERE candidate_identity=? AND candidate_artifact_root=?
                  AND current_source_content_root=? AND pit_snapshot_digest=?
                  AND semantic_profile_digest=? AND validation_fingerprint=?
                  AND outcome IN (
                    'CURRENT_SOURCE_EQUIVALENT',
                    'CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED'
                  )
                  AND equivalence_mode=outcome
                  AND committed=1
                ORDER BY observed_at DESC,attestation_id
                LIMIT ?
                """,
                (
                    candidate.candidate_identity,
                    candidate.artifact_root,
                    probe.snapshot.source_content_root,
                    probe.snapshot.pit_snapshot_digest,
                    self.profile.semantic_profile_digest,
                    self.validation_fingerprint,
                    MAX_ATTESTATION_ROWS + 1,
                ),
            ).fetchall()
        if len(rows) > MAX_ATTESTATION_ROWS:
            raise ResolutionCatalogConflict("candidate attestation history exceeds its read bound")
        reusable = [
            dict(row)
            for row in rows
            if _parse_utc(row["observed_at"]) <= observed and _parse_utc(row["valid_until"]) > observed
        ]
        if not reusable:
            return None
        if len(reusable) > 1 and (reusable[0]["observed_at"] == reusable[1]["observed_at"]):
            raise ResolutionCatalogConflict("latest reusable attestation observation is ambiguous")
        row = reusable[0]
        receipt_ref = self.cas.verify(str(row["receipt_ref"]))
        receipt = self.cas.get_json_bounded(receipt_ref, max_bytes=MAX_CONTROL_ARTIFACT_BYTES)
        if not isinstance(receipt, Mapping):
            raise ResolutionCandidateEvidenceInvalid("reusable attestation receipt is not a mapping")
        self._validate_reusable_attestation_receipt(row, receipt)
        prior_probe_ref = self.cas.verify(str(row["source_probe_ref"]))
        prior_probe = self.cas.get_json_bounded(prior_probe_ref, max_bytes=MAX_CONTROL_ARTIFACT_BYTES)
        self._validate_prior_probe_receipt(row, prior_probe)
        try:
            outcome = EquivalenceMode(str(row["outcome"]))
        except ValueError as exc:
            raise ResolutionCandidateEvidenceInvalid("reusable attestation outcome is invalid") from exc
        if outcome not in {
            EquivalenceMode.CURRENT_SOURCE_EQUIVALENT,
            EquivalenceMode.CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED,
        }:
            return None
        if row["source_probe_key"] == probe.source_probe_key and row["source_probe_ref"] == probe.cas_ref.sha256:
            return FreshAttestation(
                result=self._attestation_result(
                    row,
                    receipt_ref=receipt_ref,
                    outcome=outcome,
                ),
                renewal=None,
            )

        valid_until = min(_parse_utc(row["valid_until"]), probe.valid_until)
        if valid_until <= observed or valid_until <= probe.observed_at:
            return None
        target_key = str(row["attestation_target_key"])
        observation_key = attestation_observation_key(
            target_key,
            probe.source_probe_key,
        )
        attestation_id = f"dsat_{observation_key}"
        renewal_receipt = {
            "schema_version": ATTESTATION_RENEWAL_SCHEMA,
            "attestation_key": observation_key,
            "attestation_observation_key": observation_key,
            "attestation_target_key": target_key,
            "candidate_identity": candidate.candidate_identity,
            "candidate_artifact_root": candidate.artifact_root,
            "producer_provenance_state": row["producer_provenance_state"],
            "producer_provenance_digest_or_sentinel": row["producer_provenance_digest_or_sentinel"],
            "current_source_content_root": probe.snapshot.source_content_root,
            "source_probe_key": probe.source_probe_key,
            "source_probe_ref": probe.cas_ref.sha256,
            "pit_snapshot_digest": probe.snapshot.pit_snapshot_digest,
            "semantic_profile_digest": self.profile.semantic_profile_digest,
            "validation_fingerprint": self.validation_fingerprint,
            "observed_at": _iso(probe.observed_at),
            "valid_until": _iso(valid_until),
            "equivalence_mode": outcome.value,
            "outcome": outcome.value,
            "eligible_for_noop_reuse": True,
            "renewed_from": {
                "attestation_key": row["attestation_key"],
                "attestation_ref": receipt_ref.sha256,
                "source_probe_key": row["source_probe_key"],
                "source_probe_ref": row["source_probe_ref"],
                "observed_at": _iso(_parse_utc(row["observed_at"])),
                "valid_until": _iso(_parse_utc(row["valid_until"])),
                "validity_extended": False,
            },
            "read_only": {
                "candidate_reads": 0,
                "candidate_writes": 0,
            },
            "safety": dict(_ZERO_SAFETY),
        }
        renewed_ref = self.cas.put_json(renewal_receipt)
        self.cas.verify(renewed_ref)
        observation = AttestationObservationSpec(
            attestation_id=attestation_id,
            attestation_key=observation_key,
            attestation_target_key=target_key,
            subject_type=str(row["subject_type"]),
            subject_digest=str(row["subject_digest"]),
            candidate_identity=candidate.candidate_identity,
            producer_provenance_state=str(row["producer_provenance_state"]),
            producer_provenance_digest_or_sentinel=str(row["producer_provenance_digest_or_sentinel"]),
            candidate_artifact_root=candidate.artifact_root,
            current_source_content_root=probe.snapshot.source_content_root,
            source_probe_key=probe.source_probe_key,
            source_probe_ref=probe.cas_ref.sha256,
            pit_snapshot_digest=probe.snapshot.pit_snapshot_digest,
            semantic_profile_digest=self.profile.semantic_profile_digest,
            validation_fingerprint=self.validation_fingerprint,
            observed_at=probe.observed_at,
            valid_until=valid_until,
            equivalence_mode=outcome.value,
            outcome=outcome.value,
            receipt_ref=renewed_ref.sha256,
            committed=True,
        )
        result = AttestationResult(
            attestation_id=attestation_id,
            attestation_key=observation_key,
            attestation_target_key=target_key,
            candidate_identity=candidate.candidate_identity,
            receipt_ref=renewed_ref,
            artifact_root=candidate.artifact_root,
            outcome=outcome,
            eligible_for_noop_reuse=True,
            current_source_content_root=probe.snapshot.source_content_root,
            pit_snapshot_digest=probe.snapshot.pit_snapshot_digest,
            semantic_profile_digest=self.profile.semantic_profile_digest,
            validation_fingerprint=self.validation_fingerprint,
            valid_until=valid_until,
            run={},
        )
        return FreshAttestation(
            result=result,
            renewal=AttestationRenewalSpec(
                prior_attestation_key=str(row["attestation_key"]),
                prior_attestation_ref=receipt_ref.sha256,
                observation=observation,
            ),
        )

    def _attestation_result(
        self,
        row: Mapping[str, Any],
        *,
        receipt_ref: CASRef,
        outcome: EquivalenceMode,
    ) -> AttestationResult:
        return AttestationResult(
            attestation_id=str(row["attestation_id"]),
            attestation_key=str(row["attestation_key"]),
            attestation_target_key=str(row["attestation_target_key"]),
            candidate_identity=str(row["candidate_identity"]),
            receipt_ref=receipt_ref,
            artifact_root=str(row["candidate_artifact_root"]),
            outcome=outcome,
            eligible_for_noop_reuse=True,
            current_source_content_root=str(row["current_source_content_root"]),
            pit_snapshot_digest=str(row["pit_snapshot_digest"]),
            semantic_profile_digest=str(row["semantic_profile_digest"]),
            validation_fingerprint=str(row["validation_fingerprint"]),
            valid_until=_parse_utc(row["valid_until"]),
            run={},
        )

    def _validate_reusable_attestation_receipt(
        self,
        row: Mapping[str, Any],
        receipt: Mapping[str, Any],
    ) -> None:
        if receipt.get("schema_version") not in {
            ATTESTATION_SCHEMA_VERSION,
            ATTESTATION_RENEWAL_SCHEMA,
        }:
            raise ResolutionCandidateEvidenceInvalid("reusable attestation receipt schema is invalid")
        expected = {
            "attestation_key": row["attestation_key"],
            "attestation_observation_key": row["attestation_key"],
            "attestation_target_key": row["attestation_target_key"],
            "candidate_identity": row["candidate_identity"],
            "candidate_artifact_root": row["candidate_artifact_root"],
            "producer_provenance_state": row["producer_provenance_state"],
            "producer_provenance_digest_or_sentinel": row["producer_provenance_digest_or_sentinel"],
            "current_source_content_root": row["current_source_content_root"],
            "source_probe_key": row["source_probe_key"],
            "source_probe_ref": row["source_probe_ref"],
            "pit_snapshot_digest": row["pit_snapshot_digest"],
            "semantic_profile_digest": row["semantic_profile_digest"],
            "validation_fingerprint": row["validation_fingerprint"],
            "equivalence_mode": row["equivalence_mode"],
            "outcome": row["outcome"],
            "eligible_for_noop_reuse": True,
        }
        mismatch = {
            field: {"expected": value, "actual": receipt.get(field)}
            for field, value in expected.items()
            if receipt.get(field) != value
        }
        try:
            receipt_observed = _parse_utc(receipt.get("observed_at"))
            receipt_valid_until = _parse_utc(receipt.get("valid_until"))
        except (TypeError, ValueError) as exc:
            raise ResolutionCandidateEvidenceInvalid("reusable attestation receipt timestamps are invalid") from exc
        if receipt_observed != _parse_utc(row["observed_at"]):
            mismatch["observed_at"] = {
                "expected": row["observed_at"],
                "actual": receipt.get("observed_at"),
            }
        if receipt_valid_until != _parse_utc(row["valid_until"]):
            mismatch["valid_until"] = {
                "expected": row["valid_until"],
                "actual": receipt.get("valid_until"),
            }
        if mismatch:
            raise ResolutionCandidateEvidenceInvalid(
                "reusable attestation receipt identity differs",
                context=mismatch,
            )
        safety = receipt.get("safety")
        if (
            not isinstance(safety, Mapping)
            or any(safety.get(field) != value for field, value in _ZERO_SAFETY.items())
            or any(value != 0 for value in safety.values())
        ):
            raise ResolutionCandidateEvidenceInvalid("reusable attestation receipt safety boundary is invalid")

    def _validate_prior_probe_receipt(
        self,
        row: Mapping[str, Any],
        receipt: Any,
    ) -> None:
        expected = {
            "schema_version": "dataset_release_source_probe_v2",
            "candidate_identity": row["candidate_identity"],
            "artifact_root": row["candidate_artifact_root"],
            "source_content_root": row["current_source_content_root"],
            "pit_snapshot_digest": row["pit_snapshot_digest"],
            "source_probe_key": row["source_probe_key"],
        }
        if not isinstance(receipt, Mapping) or any(receipt.get(field) != value for field, value in expected.items()):
            raise ResolutionCandidateEvidenceInvalid("reusable attestation source-probe receipt identity differs")

    def _candidate_source_evidence(
        self,
        candidate: CatalogCandidate | None,
    ) -> CandidateSourceEvidence | None:
        if candidate is None:
            return None
        receipt_ref = candidate.build_receipt_ref or candidate.legacy_receipt_ref
        if receipt_ref is None:
            return None
        receipt = self.cas.get_json_bounded(
            receipt_ref,
            max_bytes=MAX_CONTROL_ARTIFACT_BYTES,
        )
        if not isinstance(receipt, Mapping):
            raise ResolutionCandidateEvidenceInvalid("candidate receipt is not a mapping")
        source_ref_value: Any = receipt.get("source_manifest_ref")
        if source_ref_value is None and isinstance(receipt.get("build_inputs"), Mapping):
            source_ref_value = receipt["build_inputs"].get("source_manifest_ref")
        if source_ref_value is None:
            return None
        source_ref = _complete_cas_ref(self.cas, source_ref_value, field="source_manifest_ref")
        manifest = self.cas.get_json_bounded(
            source_ref,
            max_bytes=MAX_CONTROL_ARTIFACT_BYTES,
        )
        if (
            not isinstance(manifest, Mapping)
            or manifest.get("schema_version") != SOURCE_MANIFEST_ARTIFACT_SCHEMA
            or not isinstance(manifest.get("partitions"), list)
        ):
            raise ResolutionCandidateEvidenceInvalid("candidate source manifest schema is invalid")
        reuse_ref: CASRef | None = None
        reuse_partitions: tuple[Mapping[str, Any], ...] = ()
        reuse_value = receipt.get("source_reuse_manifest_ref")
        if reuse_value is not None:
            reuse_ref = _complete_cas_ref(self.cas, reuse_value, field="source_reuse_manifest_ref")
            reuse = self.cas.get_json_bounded(reuse_ref, max_bytes=MAX_CONTROL_ARTIFACT_BYTES)
            if (
                not isinstance(reuse, Mapping)
                or reuse.get("schema_version") != "dataset_release_source_reuse_manifest_v1"
                or reuse.get("source_content_manifest_ref") != source_ref.as_dict()
                or not isinstance(reuse.get("partitions"), list)
            ):
                raise ResolutionCandidateEvidenceInvalid("candidate source reuse manifest schema is invalid")
            reuse_partitions = tuple(dict(item) for item in reuse["partitions"])
        component_ref: CASRef | None = None
        component_manifest: ComponentArtifactManifest | None = None
        component_value = receipt.get("component_artifact_manifest_ref")
        if component_value is None and isinstance(receipt.get("build_inputs"), Mapping):
            component_value = receipt["build_inputs"].get("component_artifact_manifest_ref")
        if component_value is not None:
            component_ref = _complete_cas_ref(
                self.cas,
                component_value,
                field="component_artifact_manifest_ref",
            )
            component_manifest = load_component_artifact_manifest(self.cas, component_ref)
            expected_component_identity = {
                "profile": candidate.profile,
                "scope": candidate.scope.value,
                "cutoff": candidate.cutoff,
                "candidate_identity": candidate.candidate_identity,
                "artifact_root": candidate.artifact_root,
            }
            mismatch = {
                field: {"expected": expected, "actual": getattr(component_manifest, field)}
                for field, expected in expected_component_identity.items()
                if getattr(component_manifest, field) != expected
            }
            declared_effective = receipt.get("artifact_ready_content_root")
            if mismatch or declared_effective != component_manifest.artifact_ready_content_root:
                raise ResolutionCandidateEvidenceInvalid(
                    "candidate component artifact identity differs",
                    context=mismatch,
                )
        raw_source_root = ensure_sha256(
            str(manifest.get("source_content_root", "")),
            field="raw_source_content_root",
        )
        return CandidateSourceEvidence(
            source_manifest_ref=source_ref,
            source_content_root=(
                component_manifest.artifact_ready_content_root if component_manifest is not None else raw_source_root
            ),
            partitions=tuple(dict(item) for item in manifest["partitions"]),
            raw_source_content_root=raw_source_root,
            source_reuse_manifest_ref=reuse_ref,
            reuse_partitions=reuse_partitions,
            component_artifact_manifest_ref=component_ref,
            component_artifact_manifest=component_manifest,
        )

    def _source_reuse_baseline(
        self,
        request: VersionedResolutionRequest,
    ) -> SourceReuseBaseline | None:
        try:
            row = self.store.latest_source_snapshot(
                profile=request.profile,
                scope=request.scope.value,
                cutoff_on_or_before=request.cutoff,
            )
        except StateConflict as exc:
            raise ResolutionCatalogConflict("latest source snapshot baseline is ambiguous") from exc
        if row is None:
            return None
        reference = self.cas.verify(str(row["source_reuse_manifest_ref"]))
        reuse = self.cas.get_json_bounded(reference, max_bytes=MAX_CONTROL_ARTIFACT_BYTES)
        if (
            not isinstance(reuse, Mapping)
            or reuse.get("schema_version") != SOURCE_REUSE_MANIFEST_SCHEMA
            or reuse.get("profile") != request.profile
            or reuse.get("source_content_root") != row["source_content_root"]
            or not isinstance(reuse.get("partitions"), list)
            or reuse.get("safety") != {**_ZERO_SAFETY, "provider_database_writes": 0, "candidate_writes": 0}
        ):
            raise ResolutionCandidateEvidenceInvalid("catalog source reuse baseline receipt is invalid")
        try:
            reuse_cutoff = date.fromisoformat(str(reuse["cutoff"]))
        except (KeyError, ValueError) as exc:
            raise ResolutionCandidateEvidenceInvalid("catalog source reuse baseline cutoff is invalid") from exc
        if reuse_cutoff > request.cutoff or reuse_cutoff.isoformat() != row["cutoff"]:
            raise ResolutionCandidateEvidenceInvalid("catalog source reuse baseline cutoff differs")
        if not all(isinstance(item, Mapping) for item in reuse["partitions"]):
            raise ResolutionCandidateEvidenceInvalid("catalog source reuse baseline partitions are invalid")
        return SourceReuseBaseline(
            source_reuse_manifest_ref=reference,
            partitions=tuple(dict(item) for item in reuse["partitions"]),
        )

    def _source_snapshot_catalog_spec(
        self,
        request: VersionedResolutionRequest,
        frozen: FrozenSourceAuthoritySnapshot,
        probe: SourceProbeReceipt,
    ) -> SourceSnapshotCatalogSpec:
        fields = {
            "profile": request.profile,
            "scope": request.scope.value,
            "cutoff": request.cutoff,
            "source_content_root": frozen.source_content_root,
            "source_provenance_root": frozen.source_provenance_root,
            "stable_source_provenance_root": frozen.stable_source_provenance_root,
            "source_content_manifest_ref": frozen.source_manifest_ref.sha256,
            "source_reuse_manifest_ref": frozen.source_reuse_manifest_ref.sha256,
            "source_refresh_audit_ref": frozen.source_audit_ref.sha256,
            "source_provenance_ref": frozen.source_provenance_ref.sha256,
            "pit_snapshot_digest": frozen.pit_snapshot_digest,
            "pit_snapshot_ref": frozen.pit_snapshot_ref.sha256,
        }
        return SourceSnapshotCatalogSpec(
            observation_id=digest_named_fields("dataset_release_source_snapshot_observation_v1", fields),
            **fields,
            observed_at=probe.observed_at,
        )

    def _should_reattest(
        self,
        request: VersionedResolutionRequest,
        candidate: CatalogCandidate,
        evidence: CandidateSourceEvidence | None,
        frozen: FrozenSourceAuthoritySnapshot,
    ) -> bool:
        if request.is_reattest:
            return True
        if candidate.cutoff != request.cutoff:
            return False
        if evidence is None:
            return candidate.lineage_anchor.startswith("LEGACY_RECEIPT:")
        effective_root, _ = _artifact_ready_roots(frozen)
        return evidence.source_content_root == effective_root

    def _reattest_plan(
        self,
        candidate: CatalogCandidate,
        probe: SourceProbeReceipt,
        frozen: FrozenSourceAuthoritySnapshot,
    ) -> tuple[ActionPlan, AttestationIdentity]:
        pit_matches = (
            candidate.pit_provenance_state is PitProvenanceState.KNOWN
            and candidate.pit_provenance_digest_or_sentinel == frozen.pit_snapshot_digest
        )
        source_evidence = self._candidate_source_evidence(candidate)
        effective_root, _ = _artifact_ready_roots(frozen)
        source_matches = source_evidence is None or source_evidence.source_content_root == effective_root
        if not pit_matches:
            mode = EquivalenceMode.ARTIFACT_VALID_ONLY
            producer_state = candidate.producer_provenance_state
        elif not source_matches:
            mode = EquivalenceMode.ARTIFACT_VALID_SOURCE_CHANGED
            producer_state = candidate.producer_provenance_state
        elif candidate.producer_provenance_state is ProducerProvenanceState.KNOWN:
            mode = EquivalenceMode.CURRENT_SOURCE_EQUIVALENT
            producer_state = ProducerProvenanceState.KNOWN
        else:
            mode = EquivalenceMode.CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED
            producer_state = ProducerProvenanceState.RECONSTRUCTED_SOURCE_ONLY
        target = AttestationIdentity(
            candidate_identity=candidate.candidate_identity,
            producer_provenance_state=producer_state,
            producer_provenance_digest_or_sentinel=(candidate.producer_provenance_digest_or_sentinel),
            artifact_root=candidate.artifact_root,
            current_source_content_root=effective_root,
            pit_digest=frozen.pit_snapshot_digest,
            semantic_profile_digest=self.profile.semantic_profile_digest,
            validation_fingerprint=self.validation_fingerprint,
            equivalence_mode=mode,
            source_probe_key=probe.source_probe_key,
        )
        return (
            ActionPlan(
                tuple(
                    ComponentPlan(
                        component=component,
                        partition_key="all",
                        action=ComponentAction.REATTEST,
                        reason="exact catalog candidate requires current-validator read-only attestation",
                        changed_fingerprints=("validation_fingerprint",),
                        invalidation_edges=(),
                        estimated_work={},
                    )
                    for component in Component
                )
            ),
            target,
        )

    def _build_plan(
        self,
        candidate: CatalogCandidate | None,
        evidence: CandidateSourceEvidence | None,
        frozen: FrozenSourceAuthoritySnapshot,
    ) -> ActionPlan:
        try:
            current_authority = load_artifact_ready_planning_authority(self.cas, self.profile, frozen)
            current = current_authority.components
        except DatasetReleaseError:
            current = {}
        baseline = evidence.component_artifact_manifest if evidence else None
        compatible = bool(
            candidate is not None
            and evidence is not None
            and baseline is not None
            and candidate.release_id
            and candidate.release_digest
            and candidate.attestation_key
            and baseline.semantic_profile_digest == self.profile.semantic_profile_digest
            and baseline.producer_fingerprint == self.producer_fingerprint
            and baseline.artifact_fingerprint == self.artifact_fingerprint
            and baseline.validation_fingerprint == self.validation_fingerprint
            and baseline.artifact_ready_content_root == evidence.source_content_root
        )
        context = None
        if compatible:
            assert candidate is not None
            assert candidate.release_id is not None
            assert candidate.release_digest is not None
            assert candidate.attestation_key is not None
            context = MixedPlannerContext(
                source_release_id=candidate.release_id,
                source_release_digest=candidate.release_digest,
                source_attestation_key=candidate.attestation_key,
                dataset_start=self.profile.start_date,
                cutoff=frozen.official_cutoff,
                current_pit_snapshot_digest=frozen.pit_snapshot_digest,
                current_pit_instruments=tuple(sorted({span.ts_code for span in frozen.pit_snapshot.spans})),
                current_pit_span_digest_by_code=pit_span_digest_by_code(frozen.pit_snapshot),
            )
        return build_mixed_action_plan(
            baseline=baseline,
            current=current,
            context=context,
            compatible=compatible,
        )

    def _build_inputs(
        self,
        *,
        request: VersionedResolutionRequest,
        candidate: CatalogCandidate | None,
        evidence: CandidateSourceEvidence | None,
        frozen: FrozenSourceAuthoritySnapshot,
        probe: SourceProbeReceipt,
        action_plan: ActionPlan,
    ) -> dict[str, Any]:
        artifact_ready = load_artifact_ready_planning_authority(self.cas, self.profile, frozen)
        predicted_new_bytes = frozen.source_cas_usage.get("predicted_remaining_new_bytes")
        if type(predicted_new_bytes) is not int or predicted_new_bytes < 0:
            raise ResolutionCandidateEvidenceInvalid("source freeze omitted the remaining candidate byte estimate")
        predicted_new_bytes = max(
            predicted_new_bytes,
            CANDIDATE_OUTPUT_PREDICTED_BYTES,
        )
        effective_root, effective_provenance = _artifact_ready_roots(frozen)
        resolved_intent_key = ResolvedIntentIdentity(
            request.logical_request_key,
            effective_root,
            frozen.pit_snapshot_digest,
        ).key
        build_inputs = {
            "schema_version": BUILD_INPUTS_SCHEMA_VERSION,
            "profile": request.profile,
            "scope": request.scope.value,
            "cutoff": request.cutoff.isoformat(),
            "logical_request_key": request.logical_request_key,
            "resolved_intent_key": resolved_intent_key,
            "semantic_profile_digest": request.semantic_profile_digest,
            "predicted_new_bytes": predicted_new_bytes,
            "source_manifest_ref": frozen.source_manifest_ref.as_dict(),
            "artifact_ready_contract_ref": artifact_ready.contract_ref.as_dict(),
            "artifact_ready_content_root": effective_root,
            "artifact_ready_provenance_root": effective_provenance,
            "provider_receipt_refs": [value.as_dict() for value in frozen.provider_receipt_refs],
            "artifact_ready_derived_source_receipt_refs": [
                value.as_dict() for value in frozen.artifact_ready_derived_source_receipt_refs
            ],
            "pit_snapshot_ref": frozen.pit_snapshot_ref.as_dict(),
            "source_snapshot": {
                "source_content_root": effective_root,
                "raw_source_content_root": frozen.source_content_root,
                "artifact_ready_content_root": effective_root,
                "artifact_ready_provenance_root": effective_provenance,
                "pit_snapshot_digest": frozen.pit_snapshot_digest,
            },
            "source_probe": {
                "subject_kind": probe.subject_kind.value,
                "subject_identity": probe.subject_identity,
                "candidate_identity": probe.candidate_identity,
                "artifact_root": probe.artifact_root,
            },
            "partitions": [
                item.as_build_input() for item in sorted(frozen.partitions, key=lambda value: value.spec.identity)
            ],
            "artifact_ready_effective_partitions": {
                component.value: [dict(item) for item in artifact_ready.components[component].partitions]
                for component in Component
            },
            "baseline": {
                "release_id": candidate.release_id if candidate else None,
                "release_digest": candidate.release_digest if candidate else None,
                "candidate_registration_id": (candidate.registration_id if candidate else None),
                "candidate_identity": candidate.candidate_identity if candidate else None,
                "artifact_root": candidate.artifact_root if candidate else None,
                "profile": candidate.profile if candidate else None,
                "scope": candidate.scope.value if candidate else None,
                "cutoff": candidate.cutoff.isoformat() if candidate else None,
                "semantic_profile_digest": (
                    evidence.component_artifact_manifest.semantic_profile_digest
                    if evidence and evidence.component_artifact_manifest
                    else None
                ),
                "producer_fingerprint": (
                    evidence.component_artifact_manifest.producer_fingerprint
                    if evidence and evidence.component_artifact_manifest
                    else None
                ),
                "artifact_fingerprint": (
                    evidence.component_artifact_manifest.artifact_fingerprint
                    if evidence and evidence.component_artifact_manifest
                    else None
                ),
                "validation_fingerprint": (
                    evidence.component_artifact_manifest.validation_fingerprint
                    if evidence and evidence.component_artifact_manifest
                    else None
                ),
                "source_content_root": (
                    evidence.component_artifact_manifest.source_content_root
                    if evidence and evidence.component_artifact_manifest
                    else None
                ),
                "artifact_ready_content_root": (
                    evidence.component_artifact_manifest.artifact_ready_content_root
                    if evidence and evidence.component_artifact_manifest
                    else None
                ),
                "pit_snapshot_digest": (
                    evidence.component_artifact_manifest.pit_snapshot_digest
                    if evidence and evidence.component_artifact_manifest
                    else None
                ),
                "allowlisted_root_id": (candidate.allowlisted_root_id if candidate else None),
                "volume_serial": candidate.volume_serial if candidate else None,
                "root_relative_path": candidate.root_relative_path if candidate else None,
                "source_manifest_ref": (evidence.source_manifest_ref.as_dict() if evidence else None),
                "component_artifact_manifest_ref": (
                    evidence.component_artifact_manifest_ref.as_dict()
                    if evidence is not None and evidence.component_artifact_manifest_ref is not None
                    else None
                ),
                "attestation_key": candidate.attestation_key if candidate else None,
                "reuse_evidence": [plan.as_dict() for plan in action_plan.actions if plan.frozen_reuse is not None],
            },
            "fingerprints": {
                "producer_fingerprint": self.producer_fingerprint,
                "artifact_fingerprint": self.artifact_fingerprint,
                "validation_fingerprint": self.validation_fingerprint,
                "sample_policy": SAMPLE_POLICY,
                "decision_schema": "dataset_release_decision_v1",
            },
            "safety": {
                **_ZERO_SAFETY,
                "provider_database_writes": 0,
                "candidate_writes": 0,
            },
        }
        if request.is_initial_migration:
            build_inputs["initial_migration_plan"] = {
                "plan_id": request.payload["plan_id"],
                "plan_digest": request.payload["plan_digest"],
                "fixed_cutoff": request.cutoff.isoformat(),
                "scope": request.scope.value,
                "sample_instruments": list(request.payload["sample_instruments"]),
                "event_windows": list(request.payload["event_windows"]),
                "index_windows": list(request.payload["index_windows"]),
                "source_identity_policy": request.payload["source_identity_policy"],
            }
        return build_inputs

    def _latest_attestation_row(
        self,
        candidate_identity: str,
    ) -> dict[str, Any] | None:
        with self.store.transaction(immediate=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM attestations
                WHERE candidate_identity=? AND committed=1
                ORDER BY observed_at DESC,attestation_id LIMIT 2
                """,
                (candidate_identity,),
            ).fetchall()
        if len(rows) == 2 and rows[0]["observed_at"] == rows[1]["observed_at"]:
            raise ResolutionCatalogConflict("latest committed candidate attestation is ambiguous")
        return dict(rows[0]) if rows else None

    def _aware_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ResolutionProcessorError("resolution processor clock is timezone-naive")
        return value.astimezone(UTC)


def build_resolution_processor(
    profile: DatasetProfile,
    store: ControlStore,
    cas: CASStore,
    *,
    source_authority: MonthlySourceAuthority | None = None,
    source_stage: ResolutionSourceStage | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> MonthlyResolutionProcessor:
    """Build the real default Worker resolution processor dependency."""

    return MonthlyResolutionProcessor(
        profile,
        store,
        cas,
        source_authority=source_authority,
        source_stage=source_stage,
        now=now,
    )


def _read_bounded_source_stage_result(
    path: Path,
    *,
    control_root: Path,
) -> Mapping[str, Any]:
    resolved = path.resolve(strict=True)
    root = control_root.resolve(strict=True)
    if not resolved.is_relative_to(root) or "attempt_runs" not in resolved.parts:
        raise ResolutionProcessorError("source-stage result path escaped control root")
    size = resolved.stat().st_size
    if not 0 < size <= MAX_SOURCE_STAGE_RESULT_BYTES:
        raise ResolutionProcessorError("source-stage result size is invalid")
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResolutionProcessorError("source-stage result is unreadable") from exc
    if not isinstance(value, Mapping):
        raise ResolutionProcessorError("source-stage result is not a mapping")
    return value


def _is_waitable_source_drift(value: Mapping[str, Any]) -> bool:
    expected_fields = {
        "schema_version",
        "error_code",
        "exception_type",
        "message_sha256",
        "context_ref",
        "safety",
    }
    expected_safety = {**_ZERO_SAFETY, "provider_database_writes": 0, "candidate_writes": 0}
    return bool(
        set(value) == expected_fields
        and value.get("schema_version") == SOURCE_STAGE_ERROR_SCHEMA
        and value.get("error_code") == ResolutionSourceDriftWaiting.code
        and value.get("exception_type") == "SourceSnapshotDriftBlocked"
        and isinstance(value.get("message_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", str(value["message_sha256"]))
        and value.get("context_ref") is None
        and value.get("safety") == expected_safety
    )


def _partition_maps(
    partitions: Iterable[Mapping[str, Any]],
) -> dict[Component, dict[str, Mapping[str, Any]]]:
    result: dict[Component, dict[str, Mapping[str, Any]]] = {component: {} for component in Component}
    for raw in partitions:
        try:
            identity = f"{raw['dataset']}:{raw['partition_key']}"
            consumers = raw.get("consumer_components") or [raw["component"]]
            for value in consumers:
                component = Component(str(value))
                if identity in result[component]:
                    raise ResolutionCandidateEvidenceInvalid(
                        "candidate source manifest has duplicate component partition"
                    )
                result[component][identity] = dict(raw)
        except (KeyError, TypeError, ValueError) as exc:
            raise ResolutionCandidateEvidenceInvalid("candidate source manifest partition is invalid") from exc
    return result


def _partition_identity_map(
    values: Mapping[str, Mapping[str, Any]],
) -> dict[str, tuple[str, str]]:
    output: dict[str, tuple[str, str]] = {}
    for identity, value in values.items():
        output[identity] = (
            ensure_sha256(str(value.get("content_digest", "")), field="content_digest"),
            ensure_sha256(str(value.get("schema_digest", "")), field="schema_digest"),
        )
    return output


def _schema_drift(
    current: Mapping[str, Mapping[str, Any]],
    previous: Mapping[str, Mapping[str, Any]],
) -> bool:
    for identity in set(current).intersection(previous):
        if current[identity].get("schema_digest") != previous[identity].get("schema_digest"):
            return True
        if current[identity].get("source_table_schema_digest") != previous[identity].get("source_table_schema_digest"):
            return True
    return False


def _append_only(
    current: Mapping[str, Mapping[str, Any]],
    previous: Mapping[str, Mapping[str, Any]],
) -> bool:
    if not set(previous) < set(current):
        return False
    return all(
        current[identity].get("content_digest") == value.get("content_digest")
        and current[identity].get("schema_digest") == value.get("schema_digest")
        for identity, value in previous.items()
    )


def _catalog_rank(candidate: CatalogCandidate) -> tuple[date, datetime, str]:
    return candidate.cutoff, candidate.observed_at, candidate.artifact_root


def _complete_cas_ref(cas: CASStore, value: Any, *, field: str) -> CASRef:
    try:
        reference = CASRef.from_value(value)
    except Exception as exc:
        raise ResolutionCandidateEvidenceInvalid(f"{field} CAS reference is invalid") from exc
    if reference.size < 0:
        raise ResolutionCandidateEvidenceInvalid(f"{field} requires a complete size/hash/relative-path CAS reference")
    verified = cas.verify(reference)
    if reference.relative_path != verified.relative_path:
        raise ResolutionCandidateEvidenceInvalid(f"{field} CAS path is not canonical")
    return verified


def _artifact_ready_roots(
    frozen: FrozenSourceAuthoritySnapshot,
) -> tuple[str, str]:
    if getattr(frozen, "artifact_ready_contract_ref", None) is None:
        raise ResolutionCandidateEvidenceInvalid("frozen source snapshot lacks an artifact-ready contract")
    effective = ensure_sha256(
        str(getattr(frozen, "artifact_ready_content_root", None) or ""),
        field="artifact_ready_content_root",
    )
    provenance = ensure_sha256(
        str(getattr(frozen, "artifact_ready_provenance_root", None) or ""),
        field="artifact_ready_provenance_root",
    )
    return effective, provenance


def _parse_utc(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ResolutionCatalogIncomplete("catalog timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ResolutionCatalogIncomplete("catalog timestamp is timezone-naive")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise IdentityConflictError("resolution timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


__all__ = [
    "CANDIDATE_EVIDENCE_SCHEMA",
    "CatalogCandidate",
    "CandidateSourceEvidence",
    "MonthlyResolutionProcessor",
    "InProcessFixtureSourceStage",
    "ResolutionSourceStage",
    "ResolutionCandidateEvidenceInvalid",
    "ResolutionCatalogConflict",
    "ResolutionCatalogIncomplete",
    "ResolutionProcessorError",
    "ResolutionRequestInvalid",
    "VersionedResolutionRequest",
    "SupervisedResolutionSourceStage",
    "build_resolution_processor",
]
