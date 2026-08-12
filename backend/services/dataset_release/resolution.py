from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping

from .attestation import AttestationResult
from .canonical import digest_named_fields, ensure_sha256
from .cas_store import CASRef, CASStore, CASStoreError
from .contracts import (
    Component,
    ComponentAction,
    OperationKind,
    ResolvedIntentIdentity,
    RunGenerationIdentity,
    SourceProbeIdentity,
    SourceProbeSubjectKind,
    SubmissionIdentity,
    attestation_observation_key,
    build_operation_target,
    catalog_candidate_probe_subject,
    canonical_request_hash,
    new_build_probe_subject,
    noop_operation_target,
    reattest_operation_target,
)
from .control_store import ControlStore, SourceSnapshotCatalogSpec
from .decision import ActionPlan, DECISION_SCHEMA_VERSION
from .errors import DecisionError, IdentityConflictError, SourceManifestError
from .lease import ClaimedAttempt, LeaseManager
from .state_machine import (
    AttestationRenewalSpec,
    DatasetReleaseStateMachine,
    IntentSpec,
    NoOpFinalizeSpec,
    ResolutionSnapshotSpec,
)
from .source_rows_codec import validate_rows_envelope


SOURCE_PROBE_SCHEMA_VERSION = "dataset_release_source_probe_v2"
NOOP_RECEIPT_SCHEMA_VERSION = "dataset_release_noop_receipt_v1"
RESOLUTION_PLAN_SCHEMA_VERSION = "dataset_release_resolution_plan_v2"
BUILD_INPUTS_SCHEMA_VERSION = "dataset_release_build_inputs_v1"
MAX_BUILD_INPUTS_EVIDENCE_BYTES = 32 * 1024 * 1024


@dataclass(frozen=True)
class SourceSnapshot:
    source_content_root: str
    source_provenance_root: str
    pit_snapshot_digest: str
    snapshot_tokens: tuple[str, ...]

    def __post_init__(self) -> None:
        for name in (
            "source_content_root",
            "source_provenance_root",
            "pit_snapshot_digest",
        ):
            ensure_sha256(getattr(self, name), field=name)
        if (
            not self.snapshot_tokens
            or any(not value.strip() for value in self.snapshot_tokens)
            or len(self.snapshot_tokens) != len(set(self.snapshot_tokens))
        ):
            raise IdentityConflictError("source snapshot requires unique non-empty snapshot tokens")


@dataclass(frozen=True)
class SourceProbeReceipt:
    logical_request_key: str
    candidate_identity: str | None
    artifact_root: str | None
    snapshot: SourceSnapshot
    probe_policy_version: str
    probe_ordinal: int
    observed_at: datetime
    valid_until: datetime
    receipt_digest: str
    source_probe_key: str
    cas_ref: CASRef
    subject_kind: SourceProbeSubjectKind
    subject_identity: str

    def is_fresh(self, *, now: datetime | None = None) -> bool:
        observed = _utc(now or datetime.now(UTC))
        return self.observed_at <= observed < self.valid_until


@dataclass(frozen=True)
class ResolutionResult:
    submission_id: str
    run: Mapping[str, Any]
    intent_key: str
    run_generation_digest: str
    receipt_ref: CASRef


class ResolutionService:
    """Thin coordinator over CAS, leases, and the transactional state machine."""

    def __init__(self, store: ControlStore, cas: CASStore) -> None:
        self.store = store
        self.cas = cas
        self.leases = LeaseManager(store)
        self.state_machine = DatasetReleaseStateMachine(store)

    def submit(
        self,
        *,
        identity: SubmissionIdentity,
        logical_request_key: str,
        request_payload: Mapping[str, Any],
        initial_event_type: str | None = None,
        response_payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        ensure_sha256(logical_request_key, field="logical_request_key")
        request_hash = canonical_request_hash(request_payload)
        reference = self.cas.put_json(
            {
                "schema_version": "dataset_release_submission_request_v1",
                "logical_request_key": logical_request_key,
                "request_hash": request_hash,
                "request": dict(request_payload),
                "safety": _zero_safety(),
            }
        )
        submission_id = (
            "dss_"
            + digest_named_fields(
                "dataset_release_submission_identity_v1",
                {
                    "principal": identity.principal,
                    "route": identity.route,
                    "idempotency_key": identity.idempotency_key,
                    "request_hash": request_hash,
                },
            )[:32]
        )
        response_ref = None
        if response_payload is not None:
            frozen_response = {
                **dict(response_payload),
                "submission_id": submission_id,
                "logical_request_key": logical_request_key,
                "state": "QUEUED_RESOLUTION",
                "run_id": None,
            }
            response_ref = self.cas.put_json(
                {
                    "schema_version": "dataset_release_submission_response_receipt_v1",
                    "response": frozen_response,
                }
            ).sha256
        submitted = self.store.submit(
            principal=identity.principal,
            route=identity.route,
            idempotency_key=identity.idempotency_key,
            request_hash=request_hash,
            logical_request_key=logical_request_key,
            request_ref=reference.sha256,
            actor=identity.principal,
            initial_event_type=initial_event_type,
            submission_id=submission_id,
            response_ref=response_ref,
        )
        if submitted.get("response_ref") is None:
            return submitted
        receipt = self.cas.get_json_bounded(
            str(submitted["response_ref"]),
            max_bytes=1024 * 1024,
        )
        if (
            not isinstance(receipt, Mapping)
            or receipt.get("schema_version") != "dataset_release_submission_response_receipt_v1"
            or not isinstance(receipt.get("response"), Mapping)
        ):
            raise IdentityConflictError("submission response receipt is invalid")
        return {**dict(receipt["response"]), "replayed": bool(submitted["replayed"])}

    def claim(
        self,
        *,
        submission_id: str,
        owner_identity: str,
        ttl_seconds: float,
        now: datetime | None = None,
    ) -> ClaimedAttempt:
        return self.leases.claim_resolution(
            submission_id=submission_id,
            owner_identity=owner_identity,
            ttl_seconds=ttl_seconds,
            now=now,
        )

    def record_source_probe(
        self,
        *,
        submission_id: str,
        claim: ClaimedAttempt,
        candidate_identity: str | None,
        artifact_root: str | None,
        snapshot: SourceSnapshot,
        probe_policy_version: str,
        probe_ordinal: int,
        observed_at: datetime,
        ttl: timedelta,
        subject_kind: SourceProbeSubjectKind | str = (SourceProbeSubjectKind.CATALOG_CANDIDATE),
        subject_identity: str | None = None,
    ) -> SourceProbeReceipt:
        logical = self.store.get_submission(submission_id)
        if logical is None:
            raise IdentityConflictError("source probe submission does not exist")
        if probe_ordinal <= 0 or ttl <= timedelta(0):
            raise IdentityConflictError("source probe ordinal/TTL must be positive")
        if not probe_policy_version.strip():
            raise IdentityConflictError("source probe policy version must be non-empty")
        observed = _utc(observed_at)
        valid_until = observed + ttl
        logical_key = ensure_sha256(str(logical["logical_request_key"]), field="logical_request_key")
        try:
            normalized_subject_kind = SourceProbeSubjectKind(subject_kind)
        except ValueError as exc:
            raise IdentityConflictError("source probe subject kind is invalid") from exc
        if normalized_subject_kind is SourceProbeSubjectKind.CATALOG_CANDIDATE:
            if candidate_identity is None or artifact_root is None:
                raise IdentityConflictError("catalog-candidate source probe requires candidate/artifact identity")
            candidate_identity = ensure_sha256(candidate_identity, field="candidate_identity")
            artifact_root = ensure_sha256(artifact_root, field="artifact_root")
            canonical_subject_identity = catalog_candidate_probe_subject(candidate_identity, artifact_root)
        else:
            if candidate_identity is not None or artifact_root is not None:
                raise IdentityConflictError("new-build source probe forbids candidate/artifact identity")
            canonical_subject_identity = new_build_probe_subject(logical_key)
        if (
            subject_identity is not None
            and ensure_sha256(subject_identity, field="subject_identity") != canonical_subject_identity
        ):
            raise IdentityConflictError("source probe subject identity does not match canonical identity")
        semantic_body = {
            "schema_version": SOURCE_PROBE_SCHEMA_VERSION,
            "probe_policy_version": probe_policy_version,
            "subject_kind": normalized_subject_kind.value,
            "subject_identity": canonical_subject_identity,
            "candidate_identity": candidate_identity,
            "artifact_root": artifact_root,
            "logical_request_key": logical_key,
            "source_content_root": snapshot.source_content_root,
            "source_provenance_root": snapshot.source_provenance_root,
            "pit_snapshot_digest": snapshot.pit_snapshot_digest,
            "snapshot_tokens": list(snapshot.snapshot_tokens),
            "probe_ordinal": probe_ordinal,
            "observed_at": _timestamp(observed),
            "valid_until": _timestamp(valid_until),
        }
        receipt_digest = digest_named_fields(
            SOURCE_PROBE_SCHEMA_VERSION,
            semantic_body,
        )
        source_probe_key = SourceProbeIdentity(
            logical_request_key=logical_key,
            candidate_identity=candidate_identity,
            artifact_root=artifact_root,
            source_content_root=snapshot.source_content_root,
            source_provenance_root=snapshot.source_provenance_root,
            pit_digest=snapshot.pit_snapshot_digest,
            probe_policy_version=probe_policy_version,
            probe_receipt_digest=receipt_digest,
            subject_kind=normalized_subject_kind,
            subject_identity=canonical_subject_identity,
        ).key
        reference = self.cas.put_json(
            {
                **semantic_body,
                "receipt_digest": receipt_digest,
                "source_probe_key": source_probe_key,
                "safety": _zero_safety(),
            }
        )
        self.cas.verify(reference)
        self.state_machine.record_resolution_snapshot(
            submission_id=submission_id,
            resolution_attempt_id=claim.attempt_id,
            resolution_fence=claim.attempt_fence,
            source_content_root=snapshot.source_content_root,
            source_provenance_root=snapshot.source_provenance_root,
            pit_snapshot_digest=snapshot.pit_snapshot_digest,
            source_probe_key=source_probe_key,
            source_probe_ref=reference.sha256,
            source_probe_ordinal=probe_ordinal,
            source_probe_valid_until=valid_until,
        )
        return SourceProbeReceipt(
            logical_request_key=logical_key,
            candidate_identity=candidate_identity,
            artifact_root=artifact_root,
            snapshot=snapshot,
            probe_policy_version=probe_policy_version,
            probe_ordinal=probe_ordinal,
            observed_at=observed,
            valid_until=valid_until,
            receipt_digest=receipt_digest,
            source_probe_key=source_probe_key,
            cas_ref=reference,
            subject_kind=normalized_subject_kind,
            subject_identity=canonical_subject_identity,
        )

    def resolve_noop(
        self,
        *,
        submission_id: str,
        claim: ClaimedAttempt,
        probe: SourceProbeReceipt,
        attestation: AttestationResult,
        producer_fingerprint: str,
        artifact_fingerprint: str,
        sample_policy: str,
        source_snapshot_catalog: SourceSnapshotCatalogSpec,
        attestation_renewal: AttestationRenewalSpec | None = None,
        now: datetime | None = None,
    ) -> ResolutionResult:
        observed = _utc(now or datetime.now(UTC))
        if probe.subject_kind is not SourceProbeSubjectKind.CATALOG_CANDIDATE:
            raise DecisionError("no-op resolution requires a catalog-candidate source probe")
        if not probe.is_fresh(now=observed):
            raise DecisionError("source probe is not fresh", code="SOURCE_PROBE_EXPIRED")
        if _utc(attestation.valid_until) <= observed:
            raise DecisionError("attestation is not fresh", code="ATTESTATION_EXPIRED")
        if not attestation.eligible_for_noop_reuse:
            raise DecisionError("attestation is not eligible for no-op/reuse")
        self.cas.verify(probe.cas_ref)
        self.cas.verify(attestation.receipt_ref)
        _validate_source_snapshot_catalog(source_snapshot_catalog, probe)
        for reference in _source_snapshot_catalog_refs(source_snapshot_catalog):
            self.cas.verify(reference)
        if (
            probe.candidate_identity != attestation.candidate_identity
            or probe.artifact_root != attestation.artifact_root
            or probe.snapshot.source_content_root != attestation.current_source_content_root
            or probe.snapshot.pit_snapshot_digest != attestation.pit_snapshot_digest
        ):
            raise IdentityConflictError("probe and attestation identities differ")
        if attestation_renewal is None:
            durable = self._attestation_row(attestation.attestation_key)
            if (
                durable["candidate_identity"] != probe.candidate_identity
                or durable["candidate_artifact_root"] != probe.artifact_root
                or durable["current_source_content_root"] != probe.snapshot.source_content_root
                or durable["pit_snapshot_digest"] != probe.snapshot.pit_snapshot_digest
                or durable["source_probe_key"] != probe.source_probe_key
                or durable["source_probe_ref"] != probe.cas_ref.sha256
                or durable["receipt_ref"] != attestation.receipt_ref.sha256
                or durable["attestation_target_key"] != attestation.attestation_target_key
            ):
                raise IdentityConflictError("attestation was not produced from this fresh probe")
        else:
            self._validate_attestation_renewal(
                attestation_renewal,
                attestation=attestation,
                probe=probe,
            )

        intent_identity = ResolvedIntentIdentity(
            probe.logical_request_key,
            probe.snapshot.source_content_root,
            probe.snapshot.pit_snapshot_digest,
        )
        operation_target = noop_operation_target(
            probe.candidate_identity,
            probe.artifact_root,
            probe.source_probe_key,
            attestation.attestation_key,
        )
        generation = RunGenerationIdentity(
            operation_kind=OperationKind.NO_OP,
            decision_schema=DECISION_SCHEMA_VERSION,
            producer_fingerprint=producer_fingerprint,
            artifact_fingerprint=artifact_fingerprint,
            validation_identity=attestation.attestation_key,
            sample_policy=sample_policy,
            operation_target=operation_target,
        ).digest
        receipt_body = {
            "schema_version": NOOP_RECEIPT_SCHEMA_VERSION,
            "run_generation_digest": generation,
            "resolved_intent_key": intent_identity.key,
            "candidate_identity": probe.candidate_identity,
            "artifact_root": probe.artifact_root,
            "source_probe_key": probe.source_probe_key,
            "source_probe_ref": probe.cas_ref.sha256,
            "attestation_key": attestation.attestation_key,
            "attestation_target_key": attestation.attestation_target_key,
            "attestation_ref": attestation.receipt_ref.sha256,
            "semantic_profile_digest": attestation.semantic_profile_digest,
            "validation_fingerprint": attestation.validation_fingerprint,
            "decision_schema": DECISION_SCHEMA_VERSION,
            "outcome": "NO_OP_VERIFIED",
            # Ownership/fence/wall-clock finalization fields are deliberately absent.
            "safety": _zero_safety(),
        }
        receipt_ref = self.cas.put_json(receipt_body)
        self.cas.verify(receipt_ref)
        intent = IntentSpec(
            logical_request_key=probe.logical_request_key,
            resolved_intent_key=intent_identity.key,
            source_content_root=probe.snapshot.source_content_root,
            source_provenance_root=probe.snapshot.source_provenance_root,
            pit_snapshot_digest=probe.snapshot.pit_snapshot_digest,
        )
        run = self.state_machine.finalize_noop(
            NoOpFinalizeSpec(
                submission_id=submission_id,
                resolution_attempt_id=claim.attempt_id,
                resolution_fence=claim.attempt_fence,
                source_probe_ordinal=probe.probe_ordinal,
                intent=intent,
                run_generation_digest=generation,
                candidate_identity=probe.candidate_identity,
                artifact_root=probe.artifact_root,
                source_probe_key=probe.source_probe_key,
                source_probe_ref=probe.cas_ref.sha256,
                attestation_key=attestation.attestation_key,
                attestation_target_key=attestation.attestation_target_key,
                attestation_ref=attestation.receipt_ref.sha256,
                semantic_profile_digest=attestation.semantic_profile_digest,
                validation_fingerprint=attestation.validation_fingerprint,
                decision_schema=DECISION_SCHEMA_VERSION,
                terminal_receipt_ref=receipt_ref.sha256,
                source_snapshot_catalog=source_snapshot_catalog,
                attestation_renewal=attestation_renewal,
            ),
            now=observed,
        )
        return ResolutionResult(
            submission_id=submission_id,
            run=run,
            intent_key=intent_identity.key,
            run_generation_digest=generation,
            receipt_ref=receipt_ref,
        )

    def _validate_attestation_renewal(
        self,
        renewal: AttestationRenewalSpec,
        *,
        attestation: AttestationResult,
        probe: SourceProbeReceipt,
    ) -> None:
        observation = renewal.observation
        self.cas.verify(renewal.prior_attestation_ref)
        expected = {
            "attestation_id": attestation.attestation_id,
            "attestation_key": attestation.attestation_key,
            "attestation_target_key": attestation.attestation_target_key,
            "candidate_identity": attestation.candidate_identity,
            "candidate_artifact_root": attestation.artifact_root,
            "current_source_content_root": attestation.current_source_content_root,
            "pit_snapshot_digest": attestation.pit_snapshot_digest,
            "semantic_profile_digest": attestation.semantic_profile_digest,
            "validation_fingerprint": attestation.validation_fingerprint,
            "receipt_ref": attestation.receipt_ref.sha256,
            "source_probe_key": probe.source_probe_key,
            "source_probe_ref": probe.cas_ref.sha256,
        }
        for field, value in expected.items():
            if getattr(observation, field) != value:
                raise IdentityConflictError(f"attestation renewal differs from no-op evidence for {field}")
        if observation.subject_type != "candidate" or (observation.subject_digest != attestation.candidate_identity):
            raise IdentityConflictError("attestation renewal subject differs")
        if observation.outcome != attestation.outcome.value or (
            observation.equivalence_mode != attestation.outcome.value
        ):
            raise IdentityConflictError("attestation renewal outcome differs")
        if _utc(observation.observed_at) != _utc(probe.observed_at):
            raise IdentityConflictError("attestation renewal observation time differs from fresh probe")
        if _utc(observation.valid_until) != _utc(attestation.valid_until):
            raise IdentityConflictError("attestation renewal validity differs")

    def resolve_action_plan(
        self,
        *,
        submission_id: str,
        claim: ClaimedAttempt,
        probe: SourceProbeReceipt,
        action_plan: ActionPlan,
        producer_fingerprint: str,
        artifact_fingerprint: str,
        validation_identity: str,
        sample_policy: str,
        attestation_target_key: str | None = None,
        build_inputs: Mapping[str, Any] | None = None,
        source_snapshot_catalog: SourceSnapshotCatalogSpec,
        now: datetime | None = None,
    ) -> ResolutionResult:
        observed = _utc(now or datetime.now(UTC))
        if not probe.is_fresh(now=observed):
            raise DecisionError("source probe is not fresh", code="SOURCE_PROBE_EXPIRED")
        _validate_source_snapshot_catalog(source_snapshot_catalog, probe)
        for reference in _source_snapshot_catalog_refs(source_snapshot_catalog):
            self.cas.verify(reference)
        if all(item.action.value == "NOOP" for item in action_plan.actions):
            raise DecisionError("all-NOOP action plan must use atomic no-op finalization")
        actions = {item.action for item in action_plan.actions}
        reattest_only = ComponentAction.REATTEST in actions and actions.issubset(
            {ComponentAction.NOOP, ComponentAction.REATTEST}
        )
        if reattest_only:
            if probe.subject_kind is not SourceProbeSubjectKind.CATALOG_CANDIDATE:
                raise DecisionError("re-attestation requires a catalog-candidate source probe")
            if build_inputs is not None:
                raise DecisionError("re-attestation plan cannot carry build inputs")
            if attestation_target_key is None:
                raise DecisionError("pure re-attestation plan requires an attestation target key")
            expected_observation = attestation_observation_key(
                attestation_target_key,
                probe.source_probe_key,
            )
            if validation_identity != expected_observation:
                raise IdentityConflictError("re-attestation validation identity is not the fresh observation key")
            operation_kind = OperationKind.REATTEST
            operation_target = reattest_operation_target(
                probe.candidate_identity,
                probe.artifact_root,
                attestation_target_key,
            )
        else:
            if attestation_target_key is not None:
                raise DecisionError("attestation target key is only valid for a pure re-attestation plan")
            expected_observation = None
            operation_kind = OperationKind.BUILD
        intent_identity = ResolvedIntentIdentity(
            probe.logical_request_key,
            probe.snapshot.source_content_root,
            probe.snapshot.pit_snapshot_digest,
        )
        canonical_build_inputs = None
        if operation_kind is OperationKind.BUILD:
            canonical_build_inputs = self._validated_build_inputs(
                build_inputs,
                probe=probe,
                resolved_intent_key=intent_identity.key,
            )
        plan_payload = {
            "schema_version": RESOLUTION_PLAN_SCHEMA_VERSION,
            "resolved_intent_key": intent_identity.key,
            "source_content_root": probe.snapshot.source_content_root,
            "source_provenance_root": (probe.snapshot.source_provenance_root if reattest_only else None),
            "pit_snapshot_digest": probe.snapshot.pit_snapshot_digest,
            "action_plan_digest": action_plan.digest,
            "operation_kind": operation_kind.value,
            "attestation_target_key": attestation_target_key,
            "attestation_observation_key": expected_observation,
            "source_probe_subject_kind": probe.subject_kind.value,
            "source_probe_subject_identity": probe.subject_identity,
            "source_probe_key": probe.source_probe_key if reattest_only else None,
            "source_probe_ref": probe.cas_ref.sha256 if reattest_only else None,
            "source_probe_cas_ref": (probe.cas_ref.as_dict() if reattest_only else None),
            "build_inputs": canonical_build_inputs,
            "actions": [
                item.as_dict()
                for item in sorted(
                    action_plan.actions,
                    key=lambda value: (value.component.value, value.partition_key),
                )
            ],
            "safety": _zero_safety(),
        }
        if not reattest_only:
            operation_target = build_operation_target(
                intent_identity.key,
                action_plan.digest,
            )
        generation = RunGenerationIdentity(
            operation_kind=operation_kind,
            decision_schema=DECISION_SCHEMA_VERSION,
            producer_fingerprint=producer_fingerprint,
            artifact_fingerprint=artifact_fingerprint,
            validation_identity=validation_identity,
            sample_policy=sample_policy,
            operation_target=operation_target,
        ).digest
        existing_plan_ref = self._existing_generation_plan_ref(
            resolved_intent_key=intent_identity.key,
            run_generation_digest=generation,
            operation_kind=operation_kind,
        )
        if existing_plan_ref is None:
            plan_ref = self.cas.put_json(plan_payload)
            self.cas.verify(plan_ref)
        else:
            plan_ref = existing_plan_ref
        intent = IntentSpec(
            logical_request_key=probe.logical_request_key,
            resolved_intent_key=intent_identity.key,
            source_content_root=probe.snapshot.source_content_root,
            source_provenance_root=probe.snapshot.source_provenance_root,
            pit_snapshot_digest=probe.snapshot.pit_snapshot_digest,
        )
        run = self.state_machine.create_queued_run(
            intent=intent,
            run_generation_digest=generation,
            operation_kind=operation_kind.value,
            plan_ref=plan_ref.sha256,
            submission_id=submission_id,
            resolution_attempt_id=claim.attempt_id,
            resolution_fence=claim.attempt_fence,
            expected_resolution_snapshot=ResolutionSnapshotSpec(
                source_content_root=probe.snapshot.source_content_root,
                source_provenance_root=probe.snapshot.source_provenance_root,
                pit_snapshot_digest=probe.snapshot.pit_snapshot_digest,
                source_probe_key=probe.source_probe_key,
                source_probe_ref=probe.cas_ref.sha256,
                source_probe_ordinal=probe.probe_ordinal,
                source_probe_valid_until=probe.valid_until,
            ),
            source_snapshot_catalog=source_snapshot_catalog,
        )
        return ResolutionResult(
            submission_id=submission_id,
            run=run,
            intent_key=intent_identity.key,
            run_generation_digest=generation,
            receipt_ref=plan_ref,
        )

    def _existing_generation_plan_ref(
        self,
        *,
        resolved_intent_key: str,
        run_generation_digest: str,
        operation_kind: OperationKind,
    ) -> CASRef | None:
        """Reuse the immutable authority of an equivalent in-flight generation.

        Artifact-ready provenance may change while its effective content root
        remains identical.  That must not create a second run or replace the
        first run's frozen build inputs.  The unique intent/generation row is
        the authority; a later submission links to its already verified plan.
        """

        with self.store.transaction(immediate=False) as connection:
            rows = connection.execute(
                """
                SELECT r.operation_kind,r.plan_ref
                FROM runs r
                JOIN intents i ON i.intent_id=r.intent_id
                WHERE i.resolved_intent_key=? AND r.run_generation_digest=?
                LIMIT 2
                """,
                (resolved_intent_key, run_generation_digest),
            ).fetchall()
        if not rows:
            return None
        if len(rows) != 1 or rows[0]["operation_kind"] != operation_kind.value:
            raise IdentityConflictError("existing run generation is ambiguous or bound to another operation")
        try:
            reference = self.cas.verify(str(rows[0]["plan_ref"]))
            payload = self.cas.get_json_bounded(
                reference,
                max_bytes=MAX_BUILD_INPUTS_EVIDENCE_BYTES,
            )
        except CASStoreError as exc:
            raise DecisionError("existing run generation plan evidence is unavailable") from exc
        if (
            not isinstance(payload, Mapping)
            or payload.get("schema_version") != RESOLUTION_PLAN_SCHEMA_VERSION
            or payload.get("resolved_intent_key") != resolved_intent_key
            or payload.get("operation_kind") != operation_kind.value
        ):
            raise IdentityConflictError("existing run generation plan identity differs")
        return reference

    def _validated_build_inputs(
        self,
        value: Mapping[str, Any] | None,
        *,
        probe: SourceProbeReceipt,
        resolved_intent_key: str,
    ) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise DecisionError("build plan requires immutable build inputs")
        if value.get("schema_version") != BUILD_INPUTS_SCHEMA_VERSION:
            raise DecisionError("build inputs schema version is invalid")
        expected_scalar = {
            "logical_request_key": probe.logical_request_key,
            "resolved_intent_key": resolved_intent_key,
        }
        for field, expected in expected_scalar.items():
            if value.get(field) != expected:
                raise IdentityConflictError(f"build inputs {field} differs from resolution identity")
        snapshot = value.get("source_snapshot")
        if not isinstance(snapshot, Mapping):
            raise DecisionError("build inputs source snapshot is missing")
        artifact_contract_ref = self._verified_cas_ref(
            value.get("artifact_ready_contract_ref"),
            field="artifact_ready_contract_ref",
        )
        artifact_contract = self.cas.get_json_bounded(
            artifact_contract_ref,
            max_bytes=MAX_BUILD_INPUTS_EVIDENCE_BYTES,
        )
        if not isinstance(artifact_contract, Mapping):
            raise DecisionError("artifact-ready contract is not a mapping")
        raw_source_content_root = ensure_sha256(
            str(artifact_contract.get("source_content_root", "")),
            field="raw_source_content_root",
        )
        expected_snapshot = {
            "source_content_root": probe.snapshot.source_content_root,
            "raw_source_content_root": raw_source_content_root,
            "artifact_ready_content_root": probe.snapshot.source_content_root,
            "artifact_ready_provenance_root": probe.snapshot.source_provenance_root,
            "pit_snapshot_digest": probe.snapshot.pit_snapshot_digest,
        }
        if dict(snapshot) != expected_snapshot:
            raise IdentityConflictError("build inputs source snapshot differs from fresh source probe")
        if (
            artifact_contract.get("artifact_ready_content_root") != probe.snapshot.source_content_root
            or artifact_contract.get("artifact_ready_effective_content_root") != probe.snapshot.source_content_root
            or artifact_contract.get("artifact_ready_provenance_root") != probe.snapshot.source_provenance_root
            or artifact_contract.get("pit_snapshot_digest") != probe.snapshot.pit_snapshot_digest
        ):
            raise IdentityConflictError("artifact-ready contract differs from fresh source probe")
        source_probe = value.get("source_probe")
        if not isinstance(source_probe, Mapping):
            raise DecisionError("build inputs source probe is missing")
        expected_probe = {
            "subject_kind": probe.subject_kind.value,
            "subject_identity": probe.subject_identity,
            "candidate_identity": probe.candidate_identity,
            "artifact_root": probe.artifact_root,
        }
        if dict(source_probe) != expected_probe:
            raise IdentityConflictError("build inputs source probe differs from fresh source probe")
        normalized = dict(value)
        predicted_new_bytes = value.get("predicted_new_bytes")
        if type(predicted_new_bytes) is not int or predicted_new_bytes < 0:
            raise DecisionError("build inputs predicted new bytes are invalid")
        normalized["predicted_new_bytes"] = predicted_new_bytes
        for field in ("source_manifest_ref", "pit_snapshot_ref"):
            normalized[field] = self._verified_cas_ref(value.get(field), field=field)
        normalized["artifact_ready_contract_ref"] = artifact_contract_ref
        for field, contract_field in (
            ("provider_receipt_refs", "provider_receipt_refs"),
            (
                "artifact_ready_derived_source_receipt_refs",
                "derived_source_receipt_refs",
            ),
        ):
            raw_refs = value.get(field)
            if not isinstance(raw_refs, list):
                raise DecisionError(f"build inputs {field} must be a list")
            verified_refs = [
                self._verified_cas_ref(item, field=f"{field}:{position}") for position, item in enumerate(raw_refs)
            ]
            if verified_refs != list(artifact_contract.get(contract_field) or []):
                raise IdentityConflictError(f"build inputs {field} differs from artifact-ready contract")
            normalized[field] = verified_refs
        component_refs = artifact_contract.get("component_manifests")
        effective_partitions = value.get("artifact_ready_effective_partitions")
        component_names = {component.value for component in Component}
        if (
            not isinstance(component_refs, Mapping)
            or set(component_refs) != component_names
            or not isinstance(effective_partitions, Mapping)
            or set(effective_partitions) != component_names
        ):
            raise DecisionError("build inputs artifact-ready component evidence is incomplete")
        normalized_effective: dict[str, list[dict[str, Any]]] = {}
        for component in sorted(component_names):
            component_ref = self._verified_cas_ref(
                component_refs[component],
                field=f"artifact_ready_component:{component}",
            )
            component_manifest = self.cas.get_json_bounded(
                component_ref,
                max_bytes=MAX_BUILD_INPUTS_EVIDENCE_BYTES,
            )
            declared = effective_partitions[component]
            if (
                not isinstance(component_manifest, Mapping)
                or component_manifest.get("component") != component
                or component_manifest.get("source_content_root") != raw_source_content_root
                or not isinstance(declared, list)
                or not declared
                or any(not isinstance(item, Mapping) for item in declared)
                or list(component_manifest.get("effective_partitions") or []) != declared
            ):
                raise IdentityConflictError(f"artifact-ready component evidence differs: {component}")
            normalized_effective[component] = [dict(item) for item in declared]
        normalized["artifact_ready_effective_partitions"] = normalized_effective
        normalized["artifact_ready_content_root"] = probe.snapshot.source_content_root
        normalized["artifact_ready_provenance_root"] = probe.snapshot.source_provenance_root
        partitions = value.get("partitions")
        if not isinstance(partitions, list) or not partitions:
            raise DecisionError("build inputs require non-empty sealed partitions")
        normalized_partitions: list[dict[str, Any]] = []
        identities: set[tuple[str, str]] = set()
        for raw in partitions:
            if not isinstance(raw, Mapping):
                raise DecisionError("build input partition must be a mapping")
            partition = dict(raw)
            identity = (str(partition.get("dataset", "")), str(partition.get("partition_key", "")))
            if not all(identity) or identity in identities:
                raise DecisionError("build input partition identity is empty or duplicated")
            identities.add(identity)
            partition["rows_ref"] = self._verified_cas_ref(
                partition.get("rows_ref"), field=f"partition:{identity}:rows_ref"
            )
            try:
                partition.update(
                    validate_rows_envelope(
                        partition,
                        cas_size=int(partition["rows_ref"]["size"]),
                    )
                )
            except SourceManifestError as exc:
                raise DecisionError("build input partition rows envelope is invalid") from exc
            for digest_field in (
                "schema_digest",
                "content_digest",
                "merkle_root",
            ):
                ensure_sha256(str(partition.get(digest_field, "")), field=digest_field)
            if type(partition.get("row_count")) is not int or partition["row_count"] < 0:
                raise DecisionError("build input partition row count is invalid")
            columns = partition.get("columns")
            primary_keys = partition.get("primary_keys")
            if not isinstance(columns, list) or not columns or not isinstance(primary_keys, list) or not primary_keys:
                raise DecisionError("build input partition schema is incomplete")
            normalized_partitions.append(partition)
        normalized["partitions"] = sorted(
            normalized_partitions,
            key=lambda item: (str(item["component"]), str(item["dataset"]), str(item["partition_key"])),
        )
        normalized["source_probe"] = expected_probe
        normalized["source_snapshot"] = expected_snapshot
        return normalized

    def _verified_cas_ref(self, value: Any, *, field: str) -> dict[str, Any]:
        try:
            reference = CASRef.from_value(value)
            if reference.size < 0:
                raise ValueError("CAS reference size is required")
            verified = self.cas.verify(reference)
        except (CASStoreError, TypeError, ValueError) as exc:
            raise DecisionError(f"{field} is not a complete CAS reference") from exc
        if reference.relative_path != verified.relative_path:
            raise IdentityConflictError(f"{field} relative CAS path is not canonical")
        return verified.as_dict()

    def _attestation_row(self, attestation_key: str) -> Mapping[str, Any]:
        with self.store.transaction(immediate=False) as connection:
            rows = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM attestations WHERE attestation_key=? AND committed=1",
                    (attestation_key,),
                ).fetchall()
            ]
        if len(rows) != 1:
            raise IdentityConflictError("committed attestation is missing or ambiguous")
        return rows[0]


def _zero_safety() -> dict[str, int]:
    return {
        "database_writes": 0,
        "production_writes": 0,
        "production_deletes": 0,
        "production_pointer_changes": 0,
        "service_process_controls": 0,
    }


def _validate_source_snapshot_catalog(
    spec: SourceSnapshotCatalogSpec,
    probe: SourceProbeReceipt,
) -> None:
    expected_observation_id = digest_named_fields(
        "dataset_release_source_snapshot_observation_v1",
        {
            "profile": spec.profile,
            "scope": spec.scope,
            "cutoff": spec.cutoff,
            "source_content_root": spec.source_content_root,
            "source_provenance_root": spec.source_provenance_root,
            "stable_source_provenance_root": spec.stable_source_provenance_root,
            "source_content_manifest_ref": spec.source_content_manifest_ref,
            "source_reuse_manifest_ref": spec.source_reuse_manifest_ref,
            "source_refresh_audit_ref": spec.source_refresh_audit_ref,
            "source_provenance_ref": spec.source_provenance_ref,
            "pit_snapshot_digest": spec.pit_snapshot_digest,
            "pit_snapshot_ref": spec.pit_snapshot_ref,
        },
    )
    if (
        spec.observation_id != expected_observation_id
        or spec.source_content_root != probe.snapshot.source_content_root
        or spec.source_provenance_root != probe.snapshot.source_provenance_root
        or spec.pit_snapshot_digest != probe.snapshot.pit_snapshot_digest
        or _utc(spec.observed_at) != probe.observed_at
    ):
        raise IdentityConflictError("source snapshot catalog identity differs from the exact source probe")


def _source_snapshot_catalog_refs(
    spec: SourceSnapshotCatalogSpec,
) -> tuple[str, ...]:
    return (
        spec.source_content_manifest_ref,
        spec.source_reuse_manifest_ref,
        spec.source_refresh_audit_ref,
        spec.source_provenance_ref,
        spec.pit_snapshot_ref,
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise IdentityConflictError("resolution timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")
